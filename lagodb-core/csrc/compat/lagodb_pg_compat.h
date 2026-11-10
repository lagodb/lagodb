#ifndef LAGODB_PG_COMPAT_H
#define LAGODB_PG_COMPAT_H

#include "postgres.h"

/*
 * Central PostgreSQL-version boundary for lagodb-core C forks and adapters.
 *
 * Keep one compiled fork per feature and put PostgreSQL-internal differences
 * behind these predicates. Adding a new major version requires auditing all
 * sources under csrc/ before extending LAGODB_SUPPORTED_PG_MAJOR().
 */
#define LAGODB_PG16_GE (PG_VERSION_NUM >= 160000)
#define LAGODB_PG16_LT (PG_VERSION_NUM < 160000)
#define LAGODB_PG17_GE (PG_VERSION_NUM >= 170000)
#define LAGODB_PG17_LT (PG_VERSION_NUM < 170000)
#define LAGODB_PG18_GE (PG_VERSION_NUM >= 180000)
#define LAGODB_PG18_LT (PG_VERSION_NUM < 180000)

/*
 * Capabilities PostgreSQL back-patched into several major lines. Each name
 * states what the code relies on; the owning module's README records its
 * first minor releases.
 */
#define LAGODB_HAS_INSERT_PENDING_LISTS \
	(PG_VERSION_NUM >= 150002)
/* PostgreSQL 16 keeps these generated columns in ResultRelInfo instead. */
#define LAGODB_HAS_RESULT_REL_INFO_EXTRA \
	(PG_VERSION_NUM >= 150002 && PG_VERSION_NUM < 160000)
#define LAGODB_HAS_EPQ_RESULT_RELATIONS \
	(PG_VERSION_NUM >= 150004)
#define LAGODB_HAS_RI_NEED_LOCK_TAG_TUPLE \
	(PG_VERSION_NUM >= 170001 || \
	 (PG_VERSION_NUM >= 160005 && PG_VERSION_NUM < 170000) || \
	 (PG_VERSION_NUM >= 150009 && PG_VERSION_NUM < 160000))
#define LAGODB_HAS_MERGE_ROOT_WITH_CHECK \
	(PG_VERSION_NUM >= 170006 || \
	 (PG_VERSION_NUM >= 160010 && PG_VERSION_NUM < 170000) || \
	 (PG_VERSION_NUM >= 150014 && PG_VERSION_NUM < 160000))
#define LAGODB_HAS_CHECK_VALID_RESULT_REL_NEW \
	(PG_VERSION_NUM >= 170007 || \
	 (PG_VERSION_NUM >= 160011 && PG_VERSION_NUM < 170000) || \
	 (PG_VERSION_NUM >= 150015 && PG_VERSION_NUM < 160000))
#define LAGODB_HAS_COPY_WHERE_GENERATED_CHECK \
	(PG_VERSION_NUM >= 170007 || \
	 (PG_VERSION_NUM >= 160011 && PG_VERSION_NUM < 170000) || \
	 (PG_VERSION_NUM >= 150015 && PG_VERSION_NUM < 160000))

#define LAGODB_SUPPORTED_PG_MAJOR(version) \
	((version) >= 150000 && (version) < 190000)

#if !LAGODB_SUPPORTED_PG_MAJOR(PG_VERSION_NUM)
#error "LagoDB C forks support PostgreSQL 15, 16, 17 and 18"
#endif

#endif
