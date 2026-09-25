/*
 * lagodb_vacuum_probe.c
 *      Target-lock-free VACUUM/ANALYZE provider probe.
 *
 * The caller prepares and validates the command before this classifier uses
 * NoLock for target relations. PostgreSQL name resolution still checks schema
 * permissions and invokes namespace search hooks; NoLock suppresses only the
 * target relation lock. A database-wide scan takes the normal catalog relation
 * lock. A negative result passes the original statement to the parent utility
 * chain after command validation. This is not an execution plan: a consumed
 * command repeats expansion and ownership classification under execution locks.
 *
 * KNOWN CORRECTNESS LIMIT: a negative result is not protected from concurrent
 * DDL. After this function observes a native relation, another backend can
 * drop it and create a provider-owned partitioned table with the same name before
 * the parent ProcessUtility path resolves and locks the target. The parent
 * then executes PostgreSQL's storage-less partitioned table behavior and silently
 * omits provider maintenance. The locked classification cannot repair this
 * case because a negative result never enters the LagoDB executor.
 *
 * NoLock remains the best available extension-only compromise while LagoDB
 * neither patches PostgreSQL nor requires a ProcessUtility hook load order.
 * Always consuming the command hides native-only statements from the captured
 * parent hook. Retaining probe locks is not an equivalent replacement:
 *
 * - PostgreSQL vacuum.c's expand_vacuum_rel() takes only a transient AccessShareLock
 *   on the explicit target, releases it before execution, and deliberately
 *   uses NoLock for descendants to avoid holding multiple target locks.
 * - Keeping a probe AccessShareLock through parent execution adds a lock
 *   upgrade to ANALYZE: A retains AccessShareLock(t); B holds ShareLock(t)
 *   and waits for AccessExclusiveLock(t); A requests ShareUpdateExclusiveLock(t)
 *   and waits for B. Both waits conflict with the other backend's held lock.
 *   PostgreSQL lock.c's LockConflicts and proc.c's ProcSleep establish this cycle.
 * - Transaction locks end at VACUUM's initial CommitTransactionCommand().
 *   Using session AccessShareLocks to retain targets across its transactions
 *   adds another upgrade deadlock: two VACUUM FULL backends both retain a
 *   read lock on t, then each requests AccessExclusiveLock(t) and waits for
 *   the other's read lock. Taking exclusive probe locks instead changes
 *   native lock availability and introduces multi-target lock ordering.
 *
 * These are cross-backend cycles, not self-deadlocks: LockCheckConflicts()
 * excludes the requesting backend's own session and transaction locks.
 * Probe locks also precede captured parent hooks, reversing the native
 * hook/target-lock order. Releasing them before fallback restores the DDL
 * window above. Keep NoLock under the current parent-hook contract rather
 * than silently changing PostgreSQL's lock lifecycle. This probe does not perform
 * maintenance permission checks, emit maintenance warnings, or execute provider
 * actions. Name resolution is not side-effect-free. See the adjacent README for
 * the full decision record.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "commands/vacuum.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "lagodb_vacuum_probe.h"

typedef struct LagodbVacuumProbeCache
{
	List	   *regular_ams;
	List	   *partitioned_ams;
} LagodbVacuumProbeCache;

static bool
lagodb_probe_one_oid(Oid relid, bits32 options,
					 LagodbVacuumProbeCallback callback,
					 LagodbVacuumProbeCache *cache,
					 bool *partitioned)
{
	HeapTuple	tuple;
	Form_pg_class classform;
	List	  **seen_ams;
	bool		claimed;

	if (!OidIsValid(relid))
		return false;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	classform = (Form_pg_class) GETSTRUCT(tuple);
	if (partitioned != NULL)
		*partitioned = classform->relkind == RELKIND_PARTITIONED_TABLE;
	seen_ams = classform->relkind == RELKIND_PARTITIONED_TABLE ?
		&cache->partitioned_ams : &cache->regular_ams;
	if (list_member_oid(*seen_ams, classform->relam))
	{
		ReleaseSysCache(tuple);
		return false;
	}
	claimed = callback(classform->relam, classform->relkind, options);
	if (!claimed)
		*seen_ams = lappend_oid(*seen_ams, classform->relam);
	ReleaseSysCache(tuple);

	return claimed;
}

static bool
lagodb_probe_oid(Oid relid, bits32 options,
				 LagodbVacuumProbeCallback callback,
				 LagodbVacuumProbeCache *cache)
{
	bool		partitioned = false;

	if (lagodb_probe_one_oid(relid, options, callback, cache,
							 &partitioned))
		return true;

	if (partitioned)
	{
		List	   *children = find_all_inheritors(relid, NoLock, NULL);
		ListCell   *cell;

		foreach(cell, children)
		{
			Oid			child = lfirst_oid(cell);

			if (child != relid &&
				lagodb_probe_one_oid(child, options, callback, cache, NULL))
			{
				list_free(children);
				return true;
			}
		}
		list_free(children);
	}

	return false;
}

static bool
lagodb_probe_database(bits32 options,
					  LagodbVacuumProbeCallback callback,
					  LagodbVacuumProbeCache *cache)
{
	Relation	pgclass = table_open(RelationRelationId, AccessShareLock);
	TableScanDesc scan = table_beginscan_catalog(pgclass, 0, NULL);
	HeapTuple	tuple;
	bool		claimed = false;

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

		if (classform->relkind != RELKIND_RELATION &&
			classform->relkind != RELKIND_MATVIEW &&
			classform->relkind != RELKIND_PARTITIONED_TABLE)
			continue;
		List	  **seen_ams = classform->relkind == RELKIND_PARTITIONED_TABLE ?
			&cache->partitioned_ams : &cache->regular_ams;

		if (list_member_oid(*seen_ams, classform->relam))
			continue;
		if (callback(classform->relam, classform->relkind, options))
		{
			claimed = true;
			break;
		}
		*seen_ams = lappend_oid(*seen_ams, classform->relam);
	}

	table_endscan(scan);
	table_close(pgclass, AccessShareLock);
	return claimed;
}

bool
lagodb_vacuum_probe(VacuumStmt *stmt, bits32 options,
					LagodbVacuumProbeCallback callback)
{
	LagodbVacuumProbeCache cache = {NIL, NIL};
	ListCell   *cell;
	bool		claimed = false;

	/* A database-wide command has no explicit relation list to inspect. */
	if (stmt->rels == NIL)
		claimed = lagodb_probe_database(options, callback, &cache);
	else
	{
		foreach(cell, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, cell);
			Oid			relid = vrel->oid;

			if (!OidIsValid(relid))
			{
				if (vrel->relation == NULL)
				{
					claimed = false;
					break;
				}
				relid = RangeVarGetRelidExtended(vrel->relation, NoLock,
												 RVR_MISSING_OK, NULL, NULL);
			}
			if (!OidIsValid(relid))
			{
				claimed = false;
				break;
			}

			if (lagodb_probe_oid(relid, options, callback, &cache))
				claimed = true;
		}
	}

	list_free(cache.regular_ams);
	list_free(cache.partitioned_ams);
	return claimed;
}
