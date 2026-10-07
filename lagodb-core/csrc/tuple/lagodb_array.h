#ifndef LAGODB_ARRAY_H
#define LAGODB_ARRAY_H

#include "postgres.h"
#include "utils/array.h"

#include "lagodb_pg_compat.h"

/*
 * LagoDB-owned ABI for a flat-array cursor, mirrored by ArrayCursor in
 * src/tuple/array.rs. PostgreSQL's array_iter remains local to the C bridge.
 * The caller retains the detoasted array and tracks the remaining elements.
 */
typedef struct LagodbArrayCursor
{
	char	   *data;
	bits8	   *bitmap;
	int			bitmask;
} LagodbArrayCursor;

/* Initialize the cursor and return the number of elements, including NULLs. */
extern int lagodb_array_cursor_setup(ArrayType *array, LagodbArrayCursor *cursor);

/*
 * The caller has established that another element exists and supplies its
 * catalog-bound layout. For this valid flat-array input, the step cannot raise
 * PostgreSQL ERROR: it only fetches a datum and advances the cursor. Rust calls
 * it without an FFI error guard; preserve this contract when changing the bridge.
 */
extern Datum lagodb_array_cursor_next(LagodbArrayCursor *cursor, int len,
									 bool by_value, char align, bool *is_null);

#endif
