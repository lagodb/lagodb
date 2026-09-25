/*
 * lagodb_vacuum.c
 *      Narrow PostgreSQL adapter for table-AM VACUUM providers.
 *
 * PostgreSQL keeps relation expansion and vacuum_rel() private to vacuum.c.
 * This file mirrors option parsing, expansion, and vacuum_rel().  The private
 * vacuum_rel() state machine is required so live AM ownership is decided at
 * the execution lock. Provider routing changes only the storage action.
 *
 * Provenance: PostgreSQL 17.10, primarily src/backend/commands/vacuum.c.
 * Every exported lagodb_* function below is a version-pinned adapter for
 * parse_vacuum_options(), expand_vacuum_rel(), and vacuum_rel() semantics that
 * PostgreSQL does not expose. Re-audit this file, its Rust declarations, and
 * the maintenance routing regression matrix before enabling another major.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "postmaster/bgworker_internals.h"
#include "storage/lmgr.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lagodb_pg_compat.h"
#include "lagodb_vacuum.h"

#if !LAGODB_PG17
#error "VACUUM/ANALYZE adapter has not been ported to this PostgreSQL major version"
#endif

static VacOptValue
lagodb_vacoptval_from_boolean(DefElem *def)
{
	return defGetBoolean(def) ? VACOPTVALUE_ENABLED : VACOPTVALUE_DISABLED;
}

void
lagodb_parse_vacuum_options(VacuumStmt *stmt, const char *query_string,
							QueryEnvironment *query_env,
							VacuumParams *params, int *ring_size_kb)
{
	bool		analyze = false;
	bool		disable_page_skipping = false;
	bool		freeze = false;
	bool		full = false;
	bool		only_database_stats = false;
	bool		process_main = true;
	bool		process_toast = true;
	bool		skip_database_stats = false;
	bool		skip_locked = false;
	bool		verbose = false;
	bool		has_buffer_usage_limit = false;
	ParseState *pstate;
	ListCell   *lc;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_string;
	pstate->p_queryEnv = query_env;

	memset(params, 0, sizeof(*params));
	params->index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params->truncate = VACOPTVALUE_UNSPECIFIED;
	params->nworkers = 0;
	params->toast_parent = InvalidOid;
	*ring_size_kb = -1;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = lfirst_node(DefElem, lc);

		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "skip_locked") == 0)
			skip_locked = defGetBoolean(opt);
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
			*ring_size_kb = result;
		}
		else if (!stmt->is_vacuumcmd)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized ANALYZE option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));
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
		else if (strcmp(opt->defname, "process_main") == 0)
			process_main = defGetBoolean(opt);
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
		else if (strcmp(opt->defname, "skip_database_stats") == 0)
			skip_database_stats = defGetBoolean(opt);
		else if (strcmp(opt->defname, "only_database_stats") == 0)
			only_database_stats = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized VACUUM option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	params->options =
		(stmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
		(verbose ? VACOPT_VERBOSE : 0) |
		(skip_locked ? VACOPT_SKIP_LOCKED : 0) |
		(analyze ? VACOPT_ANALYZE : 0) |
		(freeze ? VACOPT_FREEZE : 0) |
		(full ? VACOPT_FULL : 0) |
		(disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0) |
		(process_main ? VACOPT_PROCESS_MAIN : 0) |
		(process_toast ? VACOPT_PROCESS_TOAST : 0) |
		(skip_database_stats ? VACOPT_SKIP_DATABASE_STATS : 0) |
		(only_database_stats ? VACOPT_ONLY_DATABASE_STATS : 0);

	if (full && params->nworkers > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM FULL cannot be performed in parallel")));
	if (full && has_buffer_usage_limit && !analyze)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL")));
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

	params->freeze_min_age = freeze ? 0 : -1;
	params->freeze_table_age = freeze ? 0 : -1;
	params->multixact_freeze_min_age = freeze ? 0 : -1;
	params->multixact_freeze_table_age = freeze ? 0 : -1;
	params->is_wraparound = false;
	params->log_min_duration = -1;
	free_parsestate(pstate);
}

BufferAccessStrategy
lagodb_make_vacuum_buffer_strategy(int ring_size_kb, MemoryContext context)
{
	BufferAccessStrategy result;
	MemoryContext oldcontext;

	if (ring_size_kb < 0)
		ring_size_kb = VacuumBufferUsageLimit;

	oldcontext = MemoryContextSwitchTo(context);
	result = GetAccessStrategyWithSize(BAS_VACUUM, ring_size_kb);
	MemoryContextSwitchTo(oldcontext);
	return result;
}

void
lagodb_initialize_maintenance_costs(void)
{
	VacuumCostActive = false;
	VacuumFailsafeActive = false;
	VacuumCostBalance = 0;
	VacuumPageHit = 0;
	VacuumPageMiss = 0;
	VacuumPageDirty = 0;
	VacuumCostBalanceLocal = 0;
	VacuumSharedCostBalance = NULL;
	VacuumActiveNWorkers = NULL;
}

void
lagodb_finish_maintenance_costs(void)
{
	VacuumCostActive = false;
	VacuumFailsafeActive = false;
	VacuumCostBalance = 0;
}

void
lagodb_check_maintenance_command_state(VacuumStmt *stmt)
{
	const char *command = stmt->is_vacuumcmd ? "VACUUM" : "ANALYZE";

	check_stack_depth();

	/*
	 * ClassifyUtilityCommandAsReadOnly() marks VacuumStmt as allowed in a
	 * read-only transaction, but not in parallel mode or recovery.  These are
	 * the checks standard_ProcessUtility() performs before ExecVacuum().
	 */
	PreventCommandIfParallelMode(command);
	PreventCommandDuringRecovery(command);
}

static List *
lagodb_expand_one(VacuumRelation *vrel, MemoryContext context, bits32 options)
{
	List	   *result = NIL;
	MemoryContext oldcontext;

	if (OidIsValid(vrel->oid))
	{
		oldcontext = MemoryContextSwitchTo(context);

		/*
		 * The caller may commit between relations; never retain a pointer
		 * into the transaction-owned input utility tree.
		 */
		result = lappend(result, copyObject(vrel));
		MemoryContextSwitchTo(oldcontext);
		return result;
	}

	{
		int			rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
		Oid			relid = RangeVarGetRelidExtended(vrel->relation, AccessShareLock,
													 rvr_opts, NULL, NULL);
		HeapTuple	tuple;
		Form_pg_class classform;
		bool		include_parts;

		if (!OidIsValid(relid))
		{
			if (options & VACOPT_VACUUM)
				ereport(WARNING,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("skipping vacuum of \"%s\" --- lock not available",
								vrel->relation->relname)));
			else
				ereport(WARNING,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("skipping analyze of \"%s\" --- lock not available",
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
		include_parts = classform->relkind == RELKIND_PARTITIONED_TABLE;
		ReleaseSysCache(tuple);

		if (include_parts)
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
	}
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

			if (classform->relkind != RELKIND_RELATION &&
				classform->relkind != RELKIND_MATVIEW &&
				classform->relkind != RELKIND_PARTITIONED_TABLE)
				continue;
			if (!vacuum_is_permitted_for_relation(classform->oid, classform,
												  params->options))
				continue;
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

/*
 * PostgreSQL vacuum_rel(), with one provider-neutral dispatch at the storage action.
 * At entry and exit there is no active transaction.
 */
static bool
lagodb_vacuum_rel(Oid relid, RangeVar *relation, VacuumParams *params,
				  BufferAccessStrategy bstrategy,
				  LagodbVacuumRouteCallback route_callback,
				  LagodbVacuumProviderCallback provider_callback,
				  void *context)
{
	LOCKMODE	lmode;
	Relation	rel;
	LockRelId	lockrelid;
	Oid			priv_relid;
	Oid			toast_relid;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	VacuumParams toast_vacuum_params;
	bool		provider_route;

	Assert(params != NULL);
	memcpy(&toast_vacuum_params, params, sizeof(VacuumParams));

	StartTransactionCommand();
	if (!(params->options & VACOPT_FULL))
	{
		/* Match vacuum_rel(): publish the routine-vacuum horizon contract. */
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		MyProc->statusFlags |= PROC_IN_VACUUM;
		if (params->is_wraparound)
			MyProc->statusFlags |= PROC_VACUUM_FOR_WRAPAROUND;
		ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
		LWLockRelease(ProcArrayLock);
	}
	PushActiveSnapshot(GetTransactionSnapshot());
	CHECK_FOR_INTERRUPTS();

	lmode = (params->options & VACOPT_FULL) ?
		AccessExclusiveLock : ShareUpdateExclusiveLock;
	rel = vacuum_open_relation(relid, relation, params->options,
							   params->log_min_duration >= 0, lmode);
	if (rel == NULL)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	priv_relid = OidIsValid(params->toast_parent) ?
		params->toast_parent : RelationGetRelid(rel);
	if (!vacuum_is_permitted_for_relation(priv_relid, rel->rd_rel,
										  params->options & ~VACOPT_ANALYZE))
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot vacuum non-tables or special system tables",
						RelationGetRelationName(rel))));
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}
	if (RELATION_IS_OTHER_TEMP(rel))
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	provider_route = route_callback(rel, params);
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && !provider_route)
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return true;
	}

	lockrelid = rel->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&lockrelid, lmode);

	if (!provider_route)
	{
		if (params->index_cleanup == VACOPTVALUE_UNSPECIFIED)
		{
			StdRdOptIndexCleanup vacuum_index_cleanup;

			if (rel->rd_options == NULL)
				vacuum_index_cleanup = STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO;
			else
				vacuum_index_cleanup =
					((StdRdOptions *) rel->rd_options)->vacuum_index_cleanup;

			if (vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO)
				params->index_cleanup = VACOPTVALUE_AUTO;
			else if (vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON)
				params->index_cleanup = VACOPTVALUE_ENABLED;
			else
				params->index_cleanup = VACOPTVALUE_DISABLED;
		}

#ifdef USE_INJECTION_POINTS
		if (params->index_cleanup == VACOPTVALUE_AUTO)
			INJECTION_POINT("vacuum-index-cleanup-auto");
		else if (params->index_cleanup == VACOPTVALUE_DISABLED)
			INJECTION_POINT("vacuum-index-cleanup-disabled");
		else if (params->index_cleanup == VACOPTVALUE_ENABLED)
			INJECTION_POINT("vacuum-index-cleanup-enabled");
#endif

		if (params->truncate == VACOPTVALUE_UNSPECIFIED)
		{
			if (rel->rd_options == NULL ||
				((StdRdOptions *) rel->rd_options)->vacuum_truncate)
				params->truncate = VACOPTVALUE_ENABLED;
			else
				params->truncate = VACOPTVALUE_DISABLED;
		}

#ifdef USE_INJECTION_POINTS
		if (params->truncate == VACOPTVALUE_AUTO)
			INJECTION_POINT("vacuum-truncate-auto");
		else if (params->truncate == VACOPTVALUE_DISABLED)
			INJECTION_POINT("vacuum-truncate-disabled");
		else if (params->truncate == VACOPTVALUE_ENABLED)
			INJECTION_POINT("vacuum-truncate-enabled");
#endif

		if ((params->options & VACOPT_PROCESS_TOAST) != 0 &&
			((params->options & VACOPT_FULL) == 0 ||
			 (params->options & VACOPT_PROCESS_MAIN) == 0))
			toast_relid = rel->rd_rel->reltoastrelid;
		else
			toast_relid = InvalidOid;
	}
	else
		toast_relid = InvalidOid;

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(rel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	if (params->options & VACOPT_PROCESS_MAIN)
	{
		if (provider_route)
		{
			provider_callback(rel, params, context);
		}
		else if (params->options & VACOPT_FULL)
		{
			ClusterParams cluster_params = {0};

			relation_close(rel, NoLock);
			rel = NULL;
			if ((params->options & VACOPT_VERBOSE) != 0)
				cluster_params.options |= CLUOPT_VERBOSE;
			cluster_rel(relid, InvalidOid, &cluster_params);
		}
		else
			table_relation_vacuum(rel, params, bstrategy);
	}

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);

	if (rel)
		relation_close(rel, NoLock);
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (toast_relid != InvalidOid)
	{
		toast_vacuum_params.options |= VACOPT_PROCESS_MAIN;
		toast_vacuum_params.toast_parent = relid;
		lagodb_vacuum_rel(toast_relid, NULL, &toast_vacuum_params, bstrategy,
						  route_callback, provider_callback, context);
	}

	UnlockRelationIdForSession(&lockrelid, lmode);
	return true;
}

bool
lagodb_vacuum_relation(VacuumRelation *vrel, VacuumParams *params,
					   BufferAccessStrategy bstrategy,
					   LagodbVacuumRouteCallback route_callback,
					   LagodbVacuumProviderCallback provider_callback,
					   void *context)
{
	VacuumParams params_copy;
	bool		result;

	memcpy(&params_copy, params, sizeof(VacuumParams));
	VacuumFailsafeActive = false;
	VacuumUpdateCosts();
	PG_TRY();
	{
		result = lagodb_vacuum_rel(vrel->oid, vrel->relation, &params_copy,
								   bstrategy, route_callback,
								   provider_callback, context);
	}
	PG_FINALLY();
	{
		VacuumCostActive = false;
		VacuumFailsafeActive = false;
	}
	PG_END_TRY();
	return result;
}
