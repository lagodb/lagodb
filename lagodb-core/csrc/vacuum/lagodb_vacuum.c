/*
 * lagodb_vacuum.c
 *      Narrow PostgreSQL adapter for provider-owned VACUUM FULL.
 *
 * VACUUM FULL rewrites storage through relation_copy_for_cluster(), which a
 * table provider cannot satisfy: its data lives outside PostgreSQL relfiles.
 * PostgreSQL keeps option parsing and relation expansion private to vacuum.c,
 * so this file mirrors them, then runs the provider rewrite for provider
 * relations. Native relations and every ANALYZE phase return to PostgreSQL.
 *
 * Provenance: PostgreSQL 15.19, 16.15, 17.11 and 18.6, primarily
 * src/backend/commands/vacuum.c.
 * Every exported lagodb_* function below is a version-pinned adapter for
 * parse_vacuum_options(), expand_vacuum_rel(), and vacuum_rel() semantics that
 * PostgreSQL does not expose. Re-audit this file, its Rust declarations, and
 * the maintenance regression matrix before enabling another major.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "postmaster/bgworker_internals.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lagodb_pg_compat.h"
#include "lagodb_vacuum.h"

/*
 * PostgreSQL 17 added the MAINTAIN privilege and renamed this check. Its
 * signature is unchanged, and PostgreSQL 15 and 16 test table ownership
 * instead.
 */
#if LAGODB_PG17_LT
#define vacuum_is_permitted_for_relation vacuum_is_relation_owner
#endif

static VacOptValue
lagodb_vacoptval_from_boolean(DefElem *def)
{
	return defGetBoolean(def) ? VACOPTVALUE_ENABLED : VACOPTVALUE_DISABLED;
}

bool
lagodb_vacuum_requests_full(VacuumStmt *stmt)
{
	ListCell   *lc;
	bool		full = false;

	if (!stmt->is_vacuumcmd)
		return false;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = lfirst_node(DefElem, lc);

		if (strcmp(opt->defname, "full") == 0)
			full = defGetBoolean(opt);
	}
	return full;
}

void
lagodb_parse_vacuum_options(VacuumStmt *stmt, const char *query_string,
							QueryEnvironment *query_env,
							VacuumParams *params)
{
	bool		analyze = false;
	bool		disable_page_skipping = false;
	bool		freeze = false;
	bool		full = false;
	bool		process_toast = true;
	bool		skip_locked = false;
	bool		verbose = false;
#if LAGODB_PG16_GE
	bool		only_database_stats = false;
	bool		process_main = true;
	bool		skip_database_stats = false;
	bool		has_buffer_usage_limit = false;
#endif
	ParseState *pstate;
	ListCell   *lc;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_string;
	pstate->p_queryEnv = query_env;

	memset(params, 0, sizeof(*params));
	params->index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params->truncate = VACOPTVALUE_UNSPECIFIED;
	params->nworkers = 0;
#if LAGODB_PG17_GE
	params->toast_parent = InvalidOid;
#endif

	foreach(lc, stmt->options)
	{
		DefElem    *opt = lfirst_node(DefElem, lc);

		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "skip_locked") == 0)
			skip_locked = defGetBoolean(opt);
#if LAGODB_PG16_GE
		else if (strcmp(opt->defname, "buffer_usage_limit") == 0)
		{
			const char *hintmsg = NULL;
			int			result;
			char	   *value = defGetString(opt);

			has_buffer_usage_limit = true;
			if (!parse_int(value, &result, GUC_UNIT_KB, &hintmsg) ||
				(result != 0 &&
				 (result < MIN_BAS_VAC_RING_SIZE_KB ||
				  result > MAX_BAS_VAC_RING_SIZE_KB)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("BUFFER_USAGE_LIMIT option must be 0 or between %d kB and %d kB",
								MIN_BAS_VAC_RING_SIZE_KB,
								MAX_BAS_VAC_RING_SIZE_KB),
						 hintmsg ? errhint("%s", _(hintmsg)) : 0));
		}
#endif
		else if (strcmp(opt->defname, "analyze") == 0)
			analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "freeze") == 0)
			freeze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "full") == 0)
			full = defGetBoolean(opt);
		else if (strcmp(opt->defname, "disable_page_skipping") == 0)
			disable_page_skipping = defGetBoolean(opt);
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			if (opt->arg == NULL)
				params->index_cleanup = VACOPTVALUE_AUTO;
			else
			{
				char	   *value = defGetString(opt);

				if (pg_strcasecmp(value, "auto") == 0)
					params->index_cleanup = VACOPTVALUE_AUTO;
				else
					params->index_cleanup = lagodb_vacoptval_from_boolean(opt);
			}
		}
#if LAGODB_PG16_GE
		else if (strcmp(opt->defname, "process_main") == 0)
			process_main = defGetBoolean(opt);
#endif
		else if (strcmp(opt->defname, "process_toast") == 0)
			process_toast = defGetBoolean(opt);
		else if (strcmp(opt->defname, "truncate") == 0)
			params->truncate = lagodb_vacoptval_from_boolean(opt);
		else if (strcmp(opt->defname, "parallel") == 0)
		{
			if (opt->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel option requires a value between 0 and %d",
								MAX_PARALLEL_WORKER_LIMIT),
						 parser_errposition(pstate, opt->location)));
			params->nworkers = defGetInt32(opt);
			if (params->nworkers < 0 ||
				params->nworkers > MAX_PARALLEL_WORKER_LIMIT)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel workers for vacuum must be between 0 and %d",
								MAX_PARALLEL_WORKER_LIMIT),
						 parser_errposition(pstate, opt->location)));
			if (params->nworkers == 0)
				params->nworkers = -1;
		}
#if LAGODB_PG16_GE
		else if (strcmp(opt->defname, "skip_database_stats") == 0)
			skip_database_stats = defGetBoolean(opt);
		else if (strcmp(opt->defname, "only_database_stats") == 0)
			only_database_stats = defGetBoolean(opt);
#endif
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized VACUUM option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	params->options =
		VACOPT_VACUUM |
		(verbose ? VACOPT_VERBOSE : 0) |
		(skip_locked ? VACOPT_SKIP_LOCKED : 0) |
		(analyze ? VACOPT_ANALYZE : 0) |
		(freeze ? VACOPT_FREEZE : 0) |
		(full ? VACOPT_FULL : 0) |
		(disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0) |
#if LAGODB_PG16_GE
		(process_main ? VACOPT_PROCESS_MAIN : 0) |
		(skip_database_stats ? VACOPT_SKIP_DATABASE_STATS : 0) |
		(only_database_stats ? VACOPT_ONLY_DATABASE_STATS : 0) |
#endif
		(process_toast ? VACOPT_PROCESS_TOAST : 0);

	if (full && params->nworkers > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM FULL cannot be performed in parallel")));
#if LAGODB_PG16_GE
	if (full && has_buffer_usage_limit && !analyze)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL")));
#endif
	if (!(params->options & VACOPT_ANALYZE))
	{
		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (vrel->va_cols != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("ANALYZE option must be specified when a column list is provided")));
		}
	}
	if (full && disable_page_skipping)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")));
	if (full && !process_toast)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PROCESS_TOAST required with VACUUM FULL")));
#if LAGODB_PG16_GE
	if (only_database_stats)
	{
		if (stmt->rels != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ONLY_DATABASE_STATS cannot be specified with a list of tables")));
		if (params->options & ~(VACOPT_VACUUM | VACOPT_VERBOSE |
								VACOPT_PROCESS_MAIN | VACOPT_PROCESS_TOAST |
								VACOPT_ONLY_DATABASE_STATS))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ONLY_DATABASE_STATS cannot be specified with other VACUUM options")));
	}
#endif

	params->freeze_min_age = freeze ? 0 : -1;
	params->freeze_table_age = freeze ? 0 : -1;
	params->multixact_freeze_min_age = freeze ? 0 : -1;
	params->multixact_freeze_table_age = freeze ? 0 : -1;
	params->is_wraparound = false;
	params->log_min_duration = -1;
#if LAGODB_PG18_GE
	params->max_eager_freeze_failure_rate = vacuum_max_eager_freeze_failure_rate;
#endif
	free_parsestate(pstate);
}

void
lagodb_check_maintenance_command_state(void)
{
	check_stack_depth();

	/*
	 * ClassifyUtilityCommandAsReadOnly() marks VacuumStmt as allowed in a
	 * read-only transaction, but not in parallel mode or recovery.  These are
	 * the checks standard_ProcessUtility() performs before ExecVacuum().
	 */
	PreventCommandIfParallelMode("VACUUM");
	PreventCommandDuringRecovery("VACUUM");
}

static List *
lagodb_expand_one(VacuumRelation *vrel, MemoryContext context, bits32 options)
{
	List	   *result = NIL;
	MemoryContext oldcontext;
	int			rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
	Oid			relid = RangeVarGetRelidExtended(vrel->relation, AccessShareLock,
												 rvr_opts, NULL, NULL);
	HeapTuple	tuple;
	Form_pg_class classform;
#if LAGODB_PG18_GE
	bool		include_children;
	bool		is_partitioned_table;
#else
	bool		include_parts;
#endif

	if (!OidIsValid(relid))
	{
		ereport(WARNING,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("skipping vacuum of \"%s\" --- lock not available",
						vrel->relation->relname)));
		return result;
	}
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	classform = (Form_pg_class) GETSTRUCT(tuple);
	if (vacuum_is_permitted_for_relation(relid, classform, options))
	{
		oldcontext = MemoryContextSwitchTo(context);
		result = lappend(result,
						 makeVacuumRelation(vrel->relation, relid,
											vrel->va_cols));
		MemoryContextSwitchTo(oldcontext);
	}
#if LAGODB_PG18_GE
	include_children = vrel->relation->inh;
	is_partitioned_table =
		classform->relkind == RELKIND_PARTITIONED_TABLE;
	if (is_partitioned_table && !include_children)
		ereport(WARNING,
				(errmsg("VACUUM ONLY of partitioned table \"%s\" has no effect",
						vrel->relation->relname)));
#else
	include_parts = classform->relkind == RELKIND_PARTITIONED_TABLE;
#endif
	ReleaseSysCache(tuple);

#if LAGODB_PG18_GE
	if (include_children)
#else
	if (include_parts)
#endif
	{
		List	   *children = find_all_inheritors(relid, NoLock, NULL);
		ListCell   *cell;

		foreach(cell, children)
		{
			Oid			child = lfirst_oid(cell);

			if (child == relid)
				continue;
			oldcontext = MemoryContextSwitchTo(context);
			result = lappend(result,
							 makeVacuumRelation(NULL, child,
												vrel->va_cols));
			MemoryContextSwitchTo(oldcontext);
		}
	}
	UnlockRelationOid(relid, AccessShareLock);
	return result;
}

List *
lagodb_expand_vacuum_relations(VacuumStmt *stmt, VacuumParams *params,
							   MemoryContext context)
{
	List	   *result = NIL;
	ListCell   *cell;

	if (stmt->rels != NIL)
	{
		foreach(cell, stmt->rels)
		{
			List	   *sublist = lagodb_expand_one(lfirst_node(VacuumRelation, cell),
													context, params->options);
			MemoryContext oldcontext = MemoryContextSwitchTo(context);

			/* list_concat() can allocate a new List, not just reuse nodes. */
			result = list_concat(result, sublist);
			MemoryContextSwitchTo(oldcontext);
		}
		return result;
	}

	{
		Relation	pgclass = table_open(RelationRelationId, AccessShareLock);
		TableScanDesc scan = table_beginscan_catalog(pgclass, 0, NULL);
		HeapTuple	tuple;

		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

			/*
			 * PostgreSQL 15 checks permissions before the relkind filter, so
			 * it warns about every relation the user cannot vacuum.
			 */
#if LAGODB_PG16_LT
			if (!vacuum_is_permitted_for_relation(classform->oid, classform,
												  params->options))
				continue;
#endif
			if (classform->relkind != RELKIND_RELATION &&
				classform->relkind != RELKIND_MATVIEW &&
				classform->relkind != RELKIND_PARTITIONED_TABLE)
				continue;
#if LAGODB_PG16_GE
			if (!vacuum_is_permitted_for_relation(classform->oid, classform,
												  params->options))
				continue;
#endif
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(context);

				result = lappend(result,
								 makeVacuumRelation(NULL, classform->oid, NIL));
				MemoryContextSwitchTo(oldcontext);
			}
		}
		table_endscan(scan);
		table_close(pgclass, AccessShareLock);
	}
	return result;
}

VacuumStmt *
lagodb_make_vacuum_stmt(VacuumStmt *stmt, List *relations,
						MemoryContext context)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(context);
	VacuumStmt *result = makeNode(VacuumStmt);

	result->options = stmt->options;
	result->rels = relations;
	result->is_vacuumcmd = true;
	MemoryContextSwitchTo(oldcontext);
	return result;
}

VacuumStmt *
lagodb_make_analyze_stmt(VacuumStmt *stmt, List *relations,
						 MemoryContext context)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(context);
	VacuumStmt *result = makeNode(VacuumStmt);
	ListCell   *lc;

	/* These are the options the ANALYZE grammar shares with VACUUM. */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = lfirst_node(DefElem, lc);

		if (strcmp(opt->defname, "verbose") == 0 ||
			strcmp(opt->defname, "skip_locked") == 0 ||
			strcmp(opt->defname, "buffer_usage_limit") == 0)
			result->options = lappend(result->options, opt);
	}
	result->rels = relations;
	result->is_vacuumcmd = false;
	MemoryContextSwitchTo(oldcontext);
	return result;
}

/*
 * PostgreSQL vacuum_rel() for VACUUM FULL, with the provider rewrite in place
 * of cluster_rel(). Providers have no TOAST relation, so no session lock has
 * to span a second transaction. At entry and exit no transaction is active.
 */
bool
lagodb_vacuum_full_relation(VacuumRelation *vrel, VacuumParams *params,
							LagodbVacuumProviderCallback provider_callback,
							void *context)
{
	Relation	rel;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	CHECK_FOR_INTERRUPTS();

	rel = vacuum_open_relation(vrel->oid, vrel->relation, params->options,
							   params->log_min_duration >= 0,
							   AccessExclusiveLock);
	if (rel == NULL)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	if (!vacuum_is_permitted_for_relation(RelationGetRelid(rel), rel->rd_rel,
										  params->options & ~VACOPT_ANALYZE) ||
		RELATION_IS_OTHER_TEMP(rel))
	{
		relation_close(rel, AccessExclusiveLock);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(rel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
#if LAGODB_PG17_GE
	RestrictSearchPath();
#endif

	provider_callback(rel, params, context);

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);

	relation_close(rel, NoLock);
	PopActiveSnapshot();
	CommitTransactionCommand();
	return true;
}
