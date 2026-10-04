#ifndef LAGODB_ANALYZE_H
#define LAGODB_ANALYZE_H

#include "postgres.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"
#include "storage/buf.h"

typedef bool (*LagodbAnalyzeRouteCallback) (Relation relation,
											VacuumParams *params);

/*
 * PostgreSQL's analyze_rel() skips RELKIND_PARTITIONED_TABLE before it can invoke
 * table-AM sampling.  This derived entry point resolves provider ownership
 * from the live Relation after ShareUpdateExclusiveLock has been acquired;
 * its false route follows PostgreSQL's native relation semantics in the same
 * PostgreSQL-derived executor. The runtime maintenance command scope must be active;
 * recursion protection belongs to that command, not this relation entry point.
 */
extern void lagodb_analyze_relation(
									VacuumRelation *vrel,
									VacuumParams *params,
									bool in_outer_xact,
									BufferAccessStrategy bstrategy,
									LagodbAnalyzeRouteCallback route_callback);

#endif
