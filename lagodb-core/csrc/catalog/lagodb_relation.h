#ifndef LAGODB_RELATION_H
#define LAGODB_RELATION_H

#include "postgres.h"
#include "access/tableam.h"
#include "utils/rel.h"

/*
 * PostgreSQL keeps rd_tableam NULL for a partitioned relation.  A provider
 * that owns such a relation as one logical storage object still needs the
 * catalog-selected AM for maintenance callbacks.  The returned routine is
 * server-lifetime data; this helper never mutates the Relation cache entry.
 */
extern const TableAmRoutine *lagodb_partitioned_table_tableam(Relation relation);

/* The caller has established provider ownership and retains the planner lock. */
extern void lagodb_partitioned_table_estimate_size(Relation relation,
												   int32 *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac);

#endif
