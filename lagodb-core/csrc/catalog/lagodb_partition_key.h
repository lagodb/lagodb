#ifndef LAGODB_PARTITION_KEY_H
#define LAGODB_PARTITION_KEY_H

#include "postgres.h"
#include "nodes/primnodes.h"
#include "utils/relcache.h"

#include "lagodb_pg_compat.h"

/*
 * Narrow field access for the PartitionKeyData that pgrx exposes as opaque.
 * This API deliberately contains no storage-provider semantics.
 */

extern void *lagodb_relation_partition_key(Relation relation);
extern char lagodb_partition_key_strategy(const void *key);
extern int16 lagodb_partition_key_natts(const void *key);
extern AttrNumber lagodb_partition_key_attr(const void *key, int16 index);
extern Oid	lagodb_partition_key_type(const void *key, int16 index);
extern int32 lagodb_partition_key_typmod(const void *key, int16 index);
extern Oid	lagodb_partition_key_collation(const void *key, int16 index);
extern Expr *lagodb_partition_key_expr(const void *key, int16 index);

#endif
