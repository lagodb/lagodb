#include "postgres.h"

#include "nodes/pg_list.h"
#include "utils/partcache.h"

#include "lagodb_partition_key.h"

/*
 * pgrx-pg-sys intentionally exposes PartitionKeyData as an opaque zero-field
 * Rust type and does not bind RelationGetPartitionKey().  Mirroring this
 * backend-private struct with #[repr(C)] in Rust would make a layout drift a
 * silent ABI bug.  Keep this bridge mechanical: the C compiler reads the
 * audited PostgreSQL headers, while Rust owns lifetimes and every provider-specific
 * interpretation (including Iceberg transform lowering).
 */

static PartitionKey
checked_key(const void *key)
{
	Assert(key != NULL);
	return (PartitionKey) key;
}

static void
check_index(PartitionKey key, int16 index)
{
	Assert(index >= 0 && index < key->partnatts);
}

void *
lagodb_relation_partition_key(Relation relation)
{
	Assert(relation != NULL);
	return RelationGetPartitionKey(relation);
}

char
lagodb_partition_key_strategy(const void *key)
{
	return checked_key(key)->strategy;
}

int16
lagodb_partition_key_natts(const void *key)
{
	return checked_key(key)->partnatts;
}

AttrNumber
lagodb_partition_key_attr(const void *key_ptr, int16 index)
{
	PartitionKey key = checked_key(key_ptr);

	check_index(key, index);
	return key->partattrs[index];
}

Oid
lagodb_partition_key_type(const void *key_ptr, int16 index)
{
	PartitionKey key = checked_key(key_ptr);

	check_index(key, index);
	return key->parttypid[index];
}

int32
lagodb_partition_key_typmod(const void *key_ptr, int16 index)
{
	PartitionKey key = checked_key(key_ptr);

	check_index(key, index);
	return key->parttypmod[index];
}

Oid
lagodb_partition_key_collation(const void *key_ptr, int16 index)
{
	PartitionKey key = checked_key(key_ptr);

	check_index(key, index);
	return key->partcollation[index];
}

Expr *
lagodb_partition_key_expr(const void *key_ptr, int16 index)
{
	PartitionKey key = checked_key(key_ptr);
	int16		expression_index = 0;
	int16		current;

	check_index(key, index);
	if (key->partattrs[index] != 0)
		return NULL;

	for (current = 0; current < index; ++current)
	{
		if (key->partattrs[current] == 0)
			++expression_index;
	}

	return (Expr *) list_nth(key->partexprs, expression_index);
}
