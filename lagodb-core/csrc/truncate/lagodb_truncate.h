#ifndef LAGODB_TRUNCATE_H
#define LAGODB_TRUNCATE_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"

typedef bool (*LagodbPartitionedTableOwnershipCallback) (Relation relation);
typedef void (*LagodbPartitionedTableTruncateCallback) (Relation relation);

extern void lagodb_execute_truncate(
									TruncateStmt *stmt,
									LagodbPartitionedTableOwnershipCallback owns_partitioned_table,
									LagodbPartitionedTableTruncateCallback truncate_partitioned_table);

#endif
