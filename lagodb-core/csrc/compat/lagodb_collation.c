#include "lagodb_collation.h"

#include "utils/pg_locale.h"

bool
lagodb_collation_is_c(Oid collation)
{
	if (!OidIsValid(collation))
		return false;

#if LAGODB_PG18_GE
	return pg_newlocale_from_collation(collation)->collate_is_c;
#else
	return lc_collate_is_c(collation);
#endif
}
