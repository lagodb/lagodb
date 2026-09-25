/*
 * lagodb_truncate.c
 *      PostgreSQL TRUNCATE route and execution bridge.
 *
 * This bridge follows PostgreSQL 17.10's ExecuteTruncate() and
 * ExecuteTruncateGuts() ordering for validation, locking, sequences, triggers,
 * native storage, foreign tables, and logical-decoding WAL.  The only semantic
 * extension is that a target classified as a provider-owned partitioned table
 * replaces PostgreSQL's storage-less partitioned table step with the provider
 * action callback.
 *
 * Re-audit this file against src/backend/commands/tablecmds.c before enabling
 * another PostgreSQL major version.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_largeobject.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/acl.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "lagodb_pg_compat.h"
#include "lagodb_truncate.h"

#if !LAGODB_PG17
#error "TRUNCATE bridge has not been ported to this PostgreSQL major version"
#endif

typedef struct LagodbForeignTruncateInfo
{
	Oid			serverid;
	List	   *rels;
} LagodbForeignTruncateInfo;

typedef enum LagodbTruncateStorageRoute
{
	LAGODB_TRUNCATE_STORAGE_POSTGRESQL,
	LAGODB_TRUNCATE_STORAGE_PROVIDER
} LagodbTruncateStorageRoute;

typedef struct LagodbTruncateTarget
{
	Relation	relation;
	LagodbTruncateStorageRoute storage_route;
} LagodbTruncateTarget;

typedef struct LagodbTruncatePlan
{
	List	   *targets;
	List	   *all_rels;
	List	   *relids;
	List	   *relids_logged;
	DropBehavior behavior;
	bool		restart_seqs;
} LagodbTruncatePlan;

typedef struct LagodbTruncateState
{
	LagodbTruncatePlan plan;
	LagodbPartitionedTableOwnershipCallback owns_partitioned_table;
} LagodbTruncateState;

static void
lagodb_truncate_check_rel(Oid relid, Form_pg_class reltuple)
{
	char	   *relname = NameStr(reltuple->relname);

	if (reltuple->relkind == RELKIND_FOREIGN_TABLE)
	{
		Oid			serverid = GetForeignServerIdByRelId(relid);
		FdwRoutine *fdwroutine = GetFdwRoutineByServerId(serverid);

		if (!fdwroutine->ExecForeignTruncate)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot truncate foreign table \"%s\"", relname)));
	}
	else if (reltuple->relkind != RELKIND_RELATION &&
			 reltuple->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table", relname)));

	if (!allowSystemTableMods && IsSystemClass(relid, reltuple) &&
		(!IsBinaryUpgrade || relid != LargeObjectRelationId))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: \"%s\" is a system catalog", relname)));

	InvokeObjectTruncateHook(relid);
}

static void
lagodb_truncate_check_perms(Oid relid, Form_pg_class reltuple)
{
	char	   *relname = NameStr(reltuple->relname);
	AclResult	aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_TRUNCATE);

	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult,
					   get_relkind_objtype(reltuple->relkind),
					   relname);
}

static void
lagodb_truncate_check_activity(Relation rel)
{
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate temporary tables of other sessions")));

	CheckTableNotInUse(rel, "TRUNCATE");
}

static void
lagodb_range_var_callback_for_truncate(const RangeVar *relation,
									   Oid relid,
									   Oid old_relid,
									   void *arg)
{
	HeapTuple	tuple;

	if (!OidIsValid(relid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	lagodb_truncate_check_rel(relid, (Form_pg_class) GETSTRUCT(tuple));
	lagodb_truncate_check_perms(relid, (Form_pg_class) GETSTRUCT(tuple));
	ReleaseSysCache(tuple);
}

static List *
lagodb_lock_owned_sequences(const LagodbTruncatePlan *plan)
{
	List	   *seq_relids = NIL;
	ListCell   *cell;

	if (!plan->restart_seqs)
		return NIL;

	foreach(cell, plan->all_rels)
	{
		Relation	rel = (Relation) lfirst(cell);
		List	   *seqlist = getOwnedSequences(RelationGetRelid(rel));
		ListCell   *seqcell;

		foreach(seqcell, seqlist)
		{
			Oid			seq_relid = lfirst_oid(seqcell);
			Relation	seq_rel = relation_open(seq_relid, AccessExclusiveLock);

			if (!object_ownercheck(RelationRelationId,
								   seq_relid,
								   GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER,
							   OBJECT_SEQUENCE,
							   RelationGetRelationName(seq_rel));
			seq_relids = lappend_oid(seq_relids, seq_relid);
			relation_close(seq_rel, NoLock);
		}
	}

	return seq_relids;
}

static void
lagodb_log_truncate(const LagodbTruncatePlan *plan)
{
	xl_heap_truncate xlrec;
	Oid		   *log_relids;
	ListCell   *cell;
	int			index = 0;

	if (plan->relids_logged == NIL)
		return;

	Assert(XLogLogicalInfoActive());
	log_relids = palloc(list_length(plan->relids_logged) * sizeof(Oid));
	foreach(cell, plan->relids_logged)
		log_relids[index++] = lfirst_oid(cell);

	xlrec.dbId = MyDatabaseId;
	xlrec.nrelids = list_length(plan->relids_logged);
	xlrec.flags = 0;
	if (plan->behavior == DROP_CASCADE)
		xlrec.flags |= XLH_TRUNCATE_CASCADE;
	if (plan->restart_seqs)
		xlrec.flags |= XLH_TRUNCATE_RESTART_SEQS;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfHeapTruncate);
	XLogRegisterData((char *) log_relids,
					 list_length(plan->relids_logged) * sizeof(Oid));
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
	(void) XLogInsert(RM_HEAP_ID, XLOG_HEAP_TRUNCATE);
}

static void
lagodb_execute_truncate_plan(
							 const LagodbTruncatePlan *plan,
							 LagodbPartitionedTableTruncateCallback truncate_partitioned_table)
{
	List	   *seq_relids;
	HTAB	   *ft_htab = NULL;
	EState	   *estate;
	ResultRelInfo *result_rel_infos;
	ResultRelInfo *result_rel_info;
	SubTransactionId my_subid;
	ListCell   *cell;

	Assert(plan->all_rels != NIL);
	Assert(truncate_partitioned_table != NULL);
	check_stack_depth();

#ifdef USE_ASSERT_CHECKING
	heap_truncate_check_FKs(plan->all_rels, false);
#else
	if (plan->behavior == DROP_RESTRICT)
		heap_truncate_check_FKs(plan->all_rels, false);
#endif

	seq_relids = lagodb_lock_owned_sequences(plan);

	AfterTriggerBeginQuery();
	estate = CreateExecutorState();
	result_rel_infos =
		palloc(list_length(plan->all_rels) * sizeof(ResultRelInfo));
	result_rel_info = result_rel_infos;
	foreach(cell, plan->all_rels)
	{
		Relation	rel = (Relation) lfirst(cell);

		InitResultRelInfo(result_rel_info, rel, 0, NULL, 0);
		estate->es_opened_result_relations =
			lappend(estate->es_opened_result_relations, result_rel_info);
		result_rel_info++;
	}

	result_rel_info = result_rel_infos;
	foreach(cell, plan->all_rels)
	{
		ExecBSTruncateTriggers(estate, result_rel_info);
		result_rel_info++;
	}

	my_subid = GetCurrentSubTransactionId();
	foreach(cell, plan->targets)
	{
		LagodbTruncateTarget *target =
			(LagodbTruncateTarget *) lfirst(cell);
		Relation	rel = target->relation;

		if (target->storage_route == LAGODB_TRUNCATE_STORAGE_PROVIDER)
		{
			Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

			/*
			 * A provider root replaces only PostgreSQL's physical storage
			 * action.  Keep the storage-backed relation lifecycle on both
			 * sides of that action.  As in PostgreSQL's native branch, a
			 * relation created or assigned a new relfilenumber in this
			 * subtransaction cannot have a predicate lock conflict.
			 */
			if (rel->rd_createSubid != my_subid &&
				rel->rd_newRelfilelocatorSubid != my_subid)
				CheckTableForSerializableConflictIn(rel);
			truncate_partitioned_table(rel);
			pgstat_count_truncate(rel);
			continue;
		}
		Assert(target->storage_route == LAGODB_TRUNCATE_STORAGE_POSTGRESQL);

		if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			continue;

		if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		{
			Oid			serverid = GetForeignServerIdByRelId(RelationGetRelid(rel));
			bool		found;
			LagodbForeignTruncateInfo *ft_info;

			if (!ft_htab)
			{
				HASHCTL		hctl;

				memset(&hctl, 0, sizeof(HASHCTL));
				hctl.keysize = sizeof(Oid);
				hctl.entrysize = sizeof(LagodbForeignTruncateInfo);
				hctl.hcxt = CurrentMemoryContext;
				ft_htab = hash_create("TRUNCATE for Foreign Tables",
									  32,
									  &hctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			}

			ft_info = hash_search(ft_htab, &serverid, HASH_ENTER, &found);
			if (!found)
				ft_info->rels = NIL;
			ft_info->rels = lappend(ft_info->rels, rel);
			continue;
		}

		if (rel->rd_createSubid == my_subid ||
			rel->rd_newRelfilelocatorSubid == my_subid)
		{
			heap_truncate_one_rel(rel);
		}
		else
		{
			Oid			heap_relid;
			Oid			toast_relid;
			ReindexParams reindex_params = {0};

			CheckTableForSerializableConflictIn(rel);
			RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);
			heap_relid = RelationGetRelid(rel);
			toast_relid = rel->rd_rel->reltoastrelid;
			if (OidIsValid(toast_relid))
			{
				Relation	toast_rel =
					relation_open(toast_relid, AccessExclusiveLock);

				RelationSetNewRelfilenumber(
											toast_rel,
											toast_rel->rd_rel->relpersistence);
				table_close(toast_rel, NoLock);
			}
			reindex_relation(NULL,
							 heap_relid,
							 REINDEX_REL_PROCESS_TOAST,
							 &reindex_params);
		}

		pgstat_count_truncate(rel);
	}

	if (ft_htab)
	{
		LagodbForeignTruncateInfo *ft_info;
		HASH_SEQ_STATUS seq;

		hash_seq_init(&seq, ft_htab);
		PG_TRY();
		{
			while ((ft_info = hash_seq_search(&seq)) != NULL)
			{
				FdwRoutine *routine =
					GetFdwRoutineByServerId(ft_info->serverid);

				Assert(routine->ExecForeignTruncate != NULL);
				routine->ExecForeignTruncate(ft_info->rels,
											 plan->behavior,
											 plan->restart_seqs);
			}
		}
		PG_FINALLY();
		{
			hash_destroy(ft_htab);
		}
		PG_END_TRY();
	}

	foreach(cell, seq_relids)
		ResetSequence(lfirst_oid(cell));

	/*
	 * TODO(logical-replication): this native heap TRUNCATE record also names
	 * provider-owned partitioned tables, but PostgreSQL's subscriber apply
	 * worker calls ExecuteTruncateGuts() directly and therefore bypasses this
	 * bridge. ExecuteTruncateGuts() skips partitioned tables, so provider
	 * storage is not truncated on the subscriber.  Before logical replication
	 * is supported for provider-owned partitioned tables, reject
	 * publishing/applying those roots instead of allowing a silent no-op.
	 */
	lagodb_log_truncate(plan);

	result_rel_info = result_rel_infos;
	foreach(cell, plan->all_rels)
	{
		ExecASTruncateTriggers(estate, result_rel_info);
		result_rel_info++;
	}

	AfterTriggerEndQuery(estate);
	FreeExecutorState(estate);
}

static void
lagodb_check_truncate_command_state(void)
{
	if (XactReadOnly || IsInParallelMode())
	{
		PreventCommandIfReadOnly("TRUNCATE TABLE");
		PreventCommandIfParallelMode("TRUNCATE TABLE");
		PreventCommandDuringRecovery("TRUNCATE TABLE");
	}
}

static void
lagodb_add_truncate_relation(LagodbTruncateState *state,
							 Relation rel)
{
	LagodbTruncatePlan *plan = &state->plan;
	LagodbTruncateTarget *target = palloc(sizeof(LagodbTruncateTarget));
	Oid			relid = RelationGetRelid(rel);
	bool		provider_owned =
		rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
		state->owns_partitioned_table(rel);

	target->relation = rel;
	target->storage_route = provider_owned ?
		LAGODB_TRUNCATE_STORAGE_PROVIDER :
		LAGODB_TRUNCATE_STORAGE_POSTGRESQL;
	plan->targets = lappend(plan->targets, target);
	plan->all_rels = lappend(plan->all_rels, rel);
	plan->relids = lappend_oid(plan->relids, relid);
	if (RelationIsLogicallyLogged(rel))
		plan->relids_logged = lappend_oid(plan->relids_logged, relid);
}

static void
lagodb_open_initial_relations(TruncateStmt *stmt,
							  LagodbTruncateState *state)
{
	ListCell   *cell;

	foreach(cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);
		Relation	rel;
		bool		recurse = rv->inh;
		Oid			relid;
		LOCKMODE	lockmode = AccessExclusiveLock;

		relid = RangeVarGetRelidExtended(rv,
										 lockmode,
										 0,
										 lagodb_range_var_callback_for_truncate,
										 NULL);
		if (list_member_oid(state->plan.relids, relid))
			continue;

		rel = relation_open(relid, NoLock);
		lagodb_truncate_check_activity(rel);
		lagodb_add_truncate_relation(state, rel);

		if (recurse)
		{
			List	   *children = find_all_inheritors(relid, lockmode, NULL);
			ListCell   *child;

			foreach(child, children)
			{
				Oid			child_relid = lfirst_oid(child);

				if (list_member_oid(state->plan.relids, child_relid))
					continue;
				rel = relation_open(child_relid, NoLock);
				if (RELATION_IS_OTHER_TEMP(rel))
				{
					relation_close(rel, lockmode);
					continue;
				}
				lagodb_truncate_check_rel(child_relid, rel->rd_rel);
				lagodb_truncate_check_activity(rel);
				lagodb_add_truncate_relation(state, rel);
			}
		}
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot truncate only a partitioned table"),
					 errhint("Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.")));
	}
}

static void
lagodb_open_cascade_relations(LagodbTruncateState *state)
{
	for (;;)
	{
		List	   *new_relids = heap_truncate_find_FKs(state->plan.relids);
		ListCell   *cell;

		if (new_relids == NIL)
			return;

		foreach(cell, new_relids)
		{
			Oid			relid = lfirst_oid(cell);
			Relation	rel = relation_open(relid, AccessExclusiveLock);

			ereport(NOTICE,
					(errmsg("truncate cascades to table \"%s\"",
							RelationGetRelationName(rel))));
			lagodb_truncate_check_rel(relid, rel->rd_rel);
			lagodb_truncate_check_perms(relid, rel->rd_rel);
			lagodb_truncate_check_activity(rel);
			lagodb_add_truncate_relation(state, rel);
		}
	}
}

static void
lagodb_close_plan_relations(const LagodbTruncatePlan *plan)
{
	ListCell   *cell;

	foreach(cell, plan->all_rels)
		relation_close((Relation) lfirst(cell), NoLock);
}

void
lagodb_execute_truncate(
						TruncateStmt *stmt,
						LagodbPartitionedTableOwnershipCallback owns_partitioned_table,
						LagodbPartitionedTableTruncateCallback truncate_partitioned_table)
{
	LagodbTruncateState state = {0};

	Assert(owns_partitioned_table != NULL);
	Assert(truncate_partitioned_table != NULL);
	check_stack_depth();
	lagodb_check_truncate_command_state();

	state.plan.behavior = stmt->behavior;
	state.plan.restart_seqs = stmt->restart_seqs;
	state.owns_partitioned_table = owns_partitioned_table;

	lagodb_open_initial_relations(stmt, &state);
	if (stmt->behavior == DROP_CASCADE)
		lagodb_open_cascade_relations(&state);

	lagodb_execute_truncate_plan(&state.plan, truncate_partitioned_table);
	lagodb_close_plan_relations(&state.plan);
}
