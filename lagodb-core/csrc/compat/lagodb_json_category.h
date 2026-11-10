#ifndef LAGODB_JSON_CATEGORY_H
#define LAGODB_JSON_CATEGORY_H

#include "lagodb_pg_compat.h"

/*
 * LagoDB's own Datum-to-JSON classification. PostgreSQL's JsonTypeCategory
 * differs across supported majors and is private in PostgreSQL 15 and 16, so
 * Rust depends on these values alone, returned as uint32 so that no enum
 * representation crosses FFI. Keep them in step with the
 * LAGODB_JSON_CATEGORY_* constants in src/datum/json/object_encoder.rs.
 */
typedef enum LagodbJsonCategory
{
	LAGODB_JSON_CATEGORY_BOOL = 0,
	LAGODB_JSON_CATEGORY_NUMERIC = 1,
	LAGODB_JSON_CATEGORY_DATE = 2,
	LAGODB_JSON_CATEGORY_TIMESTAMP = 3,
	LAGODB_JSON_CATEGORY_TIMESTAMPTZ = 4,
	LAGODB_JSON_CATEGORY_JSON = 5,
	LAGODB_JSON_CATEGORY_ARRAY = 6,
	LAGODB_JSON_CATEGORY_COMPOSITE = 7,
	LAGODB_JSON_CATEGORY_CAST = 8,
	LAGODB_JSON_CATEGORY_OTHER = 9
} LagodbJsonCategory;

extern uint32 lagodb_json_categorize_type(Oid typoid, Oid *outfuncoid);

#endif
