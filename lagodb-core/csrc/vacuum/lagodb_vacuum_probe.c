/*
 * lagodb_vacuum_probe.c
 *      Target-lock-free VACUUM FULL provider probe.
 *
 * The caller prepares and validates the command before this classifier uses
 * NoLock for target relations. PostgreSQL name resolution still checks schema
 * permissions and invokes namespace search hooks; NoLock suppresses only the
 * target relation lock. A database-wide scan takes the normal catalog relation
 * lock. A negative result passes the original statement to the parent utility
 * chain after command validation. This is not an execution plan: a consumed
 * command repeats expansion and classifies every expanded relation.
 *
 * A negative result fails closed under concurrent DDL for a provider built on
 * core's TableAccessMethod. If another backend replaces a native target with
 * such a table after this probe, PostgreSQL reaches relation_copy_for_cluster(),
 * which rejects it by default. Core's provider hierarchy guard keeps providers
 * and native tables out of one inheritance tree, so inspecting a named target
 * covers everything it expands to.
 *
 * Retaining a probe lock would add upgrade deadlocks: two VACUUM FULL backends
 * would each hold a read lock on t and wait for the other while requesting
 * AccessExclusiveLock(t). See the adjacent README for the decision record.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/vacuum.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "lagodb_vacuum_probe.h"

Oid
lagodb_vacuum_relation_am(Oid relid)
{
	HeapTuple	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	Oid			result;

	if (!HeapTupleIsValid(tuple))
		return InvalidOid;
	result = ((Form_pg_class) GETSTRUCT(tuple))->relam;
	ReleaseSysCache(tuple);
	return result;
}

static bool
lagodb_probe_database(LagodbVacuumProbeCallback callback)
{
	Relation	pgclass = table_open(RelationRelationId, AccessShareLock);
	TableScanDesc scan = table_beginscan_catalog(pgclass, 0, NULL);
	HeapTuple	tuple;
	List	   *seen_ams = NIL;
	bool		claimed = false;

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

		if (classform->relkind != RELKIND_RELATION &&
			classform->relkind != RELKIND_MATVIEW)
			continue;
		if (list_member_oid(seen_ams, classform->relam))
			continue;
		if (callback(classform->relam))
		{
			claimed = true;
			break;
		}
		seen_ams = lappend_oid(seen_ams, classform->relam);
	}

	table_endscan(scan);
	table_close(pgclass, AccessShareLock);
	list_free(seen_ams);
	return claimed;
}

bool
lagodb_vacuum_probe(VacuumStmt *stmt, LagodbVacuumProbeCallback callback)
{
	ListCell   *cell;
	bool		claimed = false;

	/* A database-wide command has no explicit relation list to inspect. */
	if (stmt->rels == NIL)
		return lagodb_probe_database(callback);

	foreach(cell, stmt->rels)
	{
		VacuumRelation *vrel = lfirst_node(VacuumRelation, cell);
		Oid			relid = RangeVarGetRelidExtended(vrel->relation, NoLock,
													 RVR_MISSING_OK, NULL, NULL);

		/* PostgreSQL reports the unresolved name for the whole statement. */
		if (!OidIsValid(relid))
			return false;
		if (callback(lagodb_vacuum_relation_am(relid)))
			claimed = true;
	}
	return claimed;
}
