#include "lagodb_index_am.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "utils/rel.h"

#include "lagodb_pg_compat.h"

/*
 * Count one index search, as each PostgreSQL index AM does when it starts
 * a search (_bt_first, gistgettuple, blgetbitmap). PostgreSQL 18 also
 * reports the count to EXPLAIN ANALYZE through scan->instrument.
 */
void
lagodb_index_scan_record_search(IndexScanDesc scan)
{
	pgstat_count_index_scan(scan->indexRelation);
#if LAGODB_PG18_GE
	if (scan->instrument)
		scan->instrument->nsearches++;
#endif
}

/*
 * Raise PostgreSQL's unique violation for the values being inserted, as
 * _bt_check_unique does, including the constraint fields set by
 * errtableconstraint().
 */
void
lagodb_index_unique_violation(Relation index, Relation heap,
							  const Datum *values, const bool *isnull)
{
	char	   *key_desc;

	/* PostgreSQL 16 declares the arrays without const; it only reads them. */
	key_desc = BuildIndexValueDescription(index, (Datum *) values,
										  (bool *) isnull);

	ereport(ERROR,
			(errcode(ERRCODE_UNIQUE_VIOLATION),
			 errmsg("duplicate key value violates unique constraint \"%s\"",
					RelationGetRelationName(index)),
			 key_desc ? errdetail("Key %s already exists.", key_desc) : 0,
			 errtableconstraint(heap, RelationGetRelationName(index))));
}
