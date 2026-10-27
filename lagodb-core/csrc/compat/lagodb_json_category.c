#include "lagodb_json_category.h"

#if LAGODB_PG17_GE

#include "utils/jsonfuncs.h"

uint32
lagodb_json_categorize_type(Oid typoid, Oid *outfuncoid)
{
	JsonTypeCategory category;

	json_categorize_type(typoid, false, &category, outfuncoid);

	switch (category)
	{
		case JSONTYPE_BOOL:
			return LAGODB_JSON_CATEGORY_BOOL;
		case JSONTYPE_NUMERIC:
			return LAGODB_JSON_CATEGORY_NUMERIC;
		case JSONTYPE_DATE:
			return LAGODB_JSON_CATEGORY_DATE;
		case JSONTYPE_TIMESTAMP:
			return LAGODB_JSON_CATEGORY_TIMESTAMP;
		case JSONTYPE_TIMESTAMPTZ:
			return LAGODB_JSON_CATEGORY_TIMESTAMPTZ;
		case JSONTYPE_JSON:
			return LAGODB_JSON_CATEGORY_JSON;
		case JSONTYPE_ARRAY:
			return LAGODB_JSON_CATEGORY_ARRAY;
		case JSONTYPE_COMPOSITE:
			return LAGODB_JSON_CATEGORY_COMPOSITE;
		case JSONTYPE_CAST:
			return LAGODB_JSON_CATEGORY_CAST;
		case JSONTYPE_OTHER:
			return LAGODB_JSON_CATEGORY_OTHER;
		default:
			elog(ERROR, "unexpected JSON type category: %d", (int) category);
	}

	pg_unreachable();
}

#else

#include "access/transam.h"
#include "catalog/pg_type.h"
#include "parser/parse_coerce.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

/*
 * Source-derived from json_categorize_type() in PostgreSQL 17.11
 * src/backend/utils/adt/jsonfuncs.c with is_jsonb = false. PostgreSQL 16
 * keeps its variant static in json.c and leaves outfuncoid unset for the
 * categories that do not call an output function.
 */
uint32
lagodb_json_categorize_type(Oid typoid, Oid *outfuncoid)
{
	bool		typisvarlena;
	Oid			castfunc;
	CoercionPathType ctype;

	/* Look through any domain */
	typoid = getBaseType(typoid);

	*outfuncoid = InvalidOid;

	switch (typoid)
	{
		case BOOLOID:
			*outfuncoid = F_BOOLOUT;
			return LAGODB_JSON_CATEGORY_BOOL;

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
			return LAGODB_JSON_CATEGORY_NUMERIC;

		case DATEOID:
			*outfuncoid = F_DATE_OUT;
			return LAGODB_JSON_CATEGORY_DATE;

		case TIMESTAMPOID:
			*outfuncoid = F_TIMESTAMP_OUT;
			return LAGODB_JSON_CATEGORY_TIMESTAMP;

		case TIMESTAMPTZOID:
			*outfuncoid = F_TIMESTAMPTZ_OUT;
			return LAGODB_JSON_CATEGORY_TIMESTAMPTZ;

		case JSONOID:
		case JSONBOID:
			getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
			return LAGODB_JSON_CATEGORY_JSON;

		default:
			break;
	}

	/* Check for arrays and composites */
	if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID
		|| typoid == ANYCOMPATIBLEARRAYOID || typoid == RECORDARRAYOID)
	{
		*outfuncoid = F_ARRAY_OUT;
		return LAGODB_JSON_CATEGORY_ARRAY;
	}

	if (type_is_rowtype(typoid))	/* includes RECORDOID */
	{
		*outfuncoid = F_RECORD_OUT;
		return LAGODB_JSON_CATEGORY_COMPOSITE;
	}

	/* Look for a cast to json if this is not a built-in type. */
	if (typoid >= FirstNormalObjectId)
	{
		ctype = find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT,
									  &castfunc);
		if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
		{
			*outfuncoid = castfunc;
			return LAGODB_JSON_CATEGORY_CAST;
		}
	}

	getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
	return LAGODB_JSON_CATEGORY_OTHER;
}

#endif
