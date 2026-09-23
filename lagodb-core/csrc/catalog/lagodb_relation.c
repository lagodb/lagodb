#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "lagodb_relation.h"

const TableAmRoutine *
lagodb_partitioned_table_tableam(Relation relation)
{
	HeapTuple	tuple;
	Form_pg_am	am;
	Oid			handler;

	Assert(relation != NULL);
	Assert(relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
	Assert(OidIsValid(relation->rd_rel->relam));
	Assert(relation->rd_tableam == NULL);

	tuple = SearchSysCache1(AMOID,
							ObjectIdGetDatum(relation->rd_rel->relam));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for access method %u",
			 relation->rd_rel->relam);
	am = (Form_pg_am) GETSTRUCT(tuple);
	handler = am->amhandler;
	ReleaseSysCache(tuple);

	return GetTableAmRoutine(handler);
}

void
lagodb_partitioned_table_estimate_size(Relation relation, int32 *attr_widths,
									   BlockNumber *pages, double *tuples,
									   double *allvisfrac)
{
	RelationData view = *relation;

	/*
	 * PostgreSQL's estimate_rel_size() dispatches storage relations to this
	 * TableAM callback, but skips partitioned tables. Borrow the same AM for
	 * an owned provider-owned partitioned table without changing its catalog
	 * kind or shared relcache entry.
	 */
	view.rd_tableam = lagodb_partitioned_table_tableam(relation);
	table_relation_estimate_size(&view, attr_widths, pages, tuples, allvisfrac);
}
