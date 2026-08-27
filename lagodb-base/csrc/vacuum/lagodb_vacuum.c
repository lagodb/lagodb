/*
 * lagodb_vacuum.c
 *      Narrow PostgreSQL adapter for table-AM VACUUM providers.
 *
 * PostgreSQL keeps relation expansion and vacuum_rel() private to vacuum.c.
 * This file mirrors only the option/expansion and per-provider transaction
 * boundaries needed to replace cluster_rel(); native work stays in PostgreSQL.
 *
 * Provenance: PostgreSQL 17.10, primarily src/backend/commands/vacuum.c. This
 * is not a copy of vacuum.c: every exported lagodb_* function below is a
 * deliberately narrow adapter for parse_vacuum_options(),
 * expand_vacuum_rel() and vacuum_rel() semantics that PostgreSQL does not
 * expose. Re-audit this file, its Rust declarations, and the FULL routing
 * regression matrix before enabling a different PostgreSQL major version.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
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
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lagodb_pg_compat.h"
#include "lagodb_vacuum.h"

#if !LAGODB_PG17
#error "VACUUM FULL adapter has not been ported to this PostgreSQL major version"
#endif

static VacOptValue
lagodb_vacoptval_from_boolean(DefElem *def)
{
    return defGetBoolean(def) ? VACOPTVALUE_ENABLED : VACOPTVALUE_DISABLED;
}

bool
lagodb_parse_vacuum_full(VacuumStmt *stmt, VacuumParams *params)
{
    bool analyze = false;
    bool disable_page_skipping = false;
    bool freeze = false;
    bool full = false;
    bool only_database_stats = false;
    bool process_main = true;
    bool process_toast = true;
    bool skip_database_stats = false;
    bool skip_locked = false;
    bool verbose = false;
    bool has_buffer_usage_limit = false;
    ListCell *lc;

    memset(params, 0, sizeof(*params));
    params->index_cleanup = VACOPTVALUE_UNSPECIFIED;
    params->truncate = VACOPTVALUE_UNSPECIFIED;
    params->nworkers = 0;
    params->toast_parent = InvalidOid;

    foreach(lc, stmt->options)
    {
        DefElem *opt = lfirst_node(DefElem, lc);

        if (strcmp(opt->defname, "verbose") == 0)
            verbose = defGetBoolean(opt);
        else if (strcmp(opt->defname, "skip_locked") == 0)
            skip_locked = defGetBoolean(opt);
        else if (strcmp(opt->defname, "buffer_usage_limit") == 0)
        {
            const char *hintmsg = NULL;
            int result;
            char *value = defGetString(opt);

            has_buffer_usage_limit = true;
            if (!parse_int(value, &result, GUC_UNIT_KB, &hintmsg) ||
                (result != 0 &&
                 (result < MIN_BAS_VAC_RING_SIZE_KB ||
                  result > MAX_BAS_VAC_RING_SIZE_KB)))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("BUFFER_USAGE_LIMIT option must be 0 or between %d kB and %d kB",
                                MIN_BAS_VAC_RING_SIZE_KB,
                                MAX_BAS_VAC_RING_SIZE_KB)));
        }
        else if (!stmt->is_vacuumcmd)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("unrecognized ANALYZE option \"%s\"", opt->defname)));
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
                char *value = defGetString(opt);

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
                         errmsg("parallel option requires a value")));
            params->nworkers = defGetInt32(opt);
            if (params->nworkers < 0 ||
                params->nworkers > MAX_PARALLEL_WORKER_LIMIT)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("parallel workers for vacuum must be between 0 and %d",
                                MAX_PARALLEL_WORKER_LIMIT)));
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
                     errmsg("unrecognized VACUUM option \"%s\"", opt->defname)));
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

    if (!stmt->is_vacuumcmd || !full)
        return false;
    if (params->nworkers > 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("VACUUM FULL cannot be performed in parallel")));
    if (has_buffer_usage_limit && !analyze)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL")));
    if (!analyze)
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
    if (disable_page_skipping)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")));
    if (!process_toast)
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
    return true;
}

static List *
lagodb_expand_one(VacuumRelation *vrel, MemoryContext context, bits32 options)
{
    List *result = NIL;
    MemoryContext oldcontext;

    if (OidIsValid(vrel->oid))
    {
        oldcontext = MemoryContextSwitchTo(context);
        result = lappend(result, vrel);
        MemoryContextSwitchTo(oldcontext);
        return result;
    }

    {
        int rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
        Oid relid = RangeVarGetRelidExtended(vrel->relation, AccessShareLock,
                                             rvr_opts, NULL, NULL);
        HeapTuple tuple;
        Form_pg_class classform;
        bool include_parts;

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
        include_parts = classform->relkind == RELKIND_PARTITIONED_TABLE;
        ReleaseSysCache(tuple);

        if (include_parts)
        {
            List *children = find_all_inheritors(relid, NoLock, NULL);
            ListCell *cell;
            foreach(cell, children)
            {
                Oid child = lfirst_oid(cell);
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
    List *result = NIL;
    ListCell *cell;

    if (stmt->rels != NIL)
    {
        foreach(cell, stmt->rels)
            result = list_concat(result,
                                 lagodb_expand_one(lfirst_node(VacuumRelation, cell),
                                                     context, params->options));
        return result;
    }

    {
        Relation pgclass = table_open(RelationRelationId, AccessShareLock);
        TableScanDesc scan = table_beginscan_catalog(pgclass, 0, NULL);
        HeapTuple tuple;
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

Oid
lagodb_relation_access_method(Oid relid)
{
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    Oid result = InvalidOid;
    if (HeapTupleIsValid(tuple))
    {
        result = ((Form_pg_class) GETSTRUCT(tuple))->relam;
        ReleaseSysCache(tuple);
    }
    return result;
}

void *
lagodb_copy_node_to_context(const void *node, MemoryContext context)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(context);
    const void *copy = copyObject(node);
    MemoryContextSwitchTo(oldcontext);
    return unconstify(void *, copy);
}

int
lagodb_vacuum_provider_relation(VacuumRelation *vrel, VacuumParams *params,
                                  LagodbVacuumProviderCallback callback,
                                  void *context)
{
    Relation rel;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;
    int result = -1;

    if (ActiveSnapshotSet())
        PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    CHECK_FOR_INTERRUPTS();

    rel = vacuum_open_relation(vrel->oid, vrel->relation, params->options,
                               (params->options & VACOPT_VERBOSE) != 0,
                               AccessExclusiveLock);
    if (rel == NULL)
        goto done;
    if (!vacuum_is_permitted_for_relation(RelationGetRelid(rel), rel->rd_rel,
                                           params->options & ~VACOPT_ANALYZE))
        goto close_done;
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_MATVIEW &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
    {
        ereport(WARNING,
                (errmsg("skipping \"%s\" --- cannot vacuum non-tables or special system tables",
                        RelationGetRelationName(rel))));
        goto close_done;
    }
    if (RELATION_IS_OTHER_TEMP(rel))
        goto close_done;
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        result = 2;
        goto close_done;
    }
    if (!(params->options & VACOPT_PROCESS_MAIN))
    {
        result = 2;
        goto close_done;
    }

    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(rel->rd_rel->relowner,
                          save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();
    result = callback(rel, params, context) ? 1 : 0;
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);

close_done:
    relation_close(rel, NoLock);
done:
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
    return result;
}
