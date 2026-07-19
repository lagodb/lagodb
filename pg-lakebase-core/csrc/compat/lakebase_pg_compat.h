#ifndef LAKEBASE_PG_COMPAT_H
#define LAKEBASE_PG_COMPAT_H

#include "postgres.h"

/*
 * Central PostgreSQL-version boundary for Lakebase C forks.
 *
 * Keep one compiled fork per feature and put PostgreSQL-internal differences
 * behind these predicates. Adding a new major version requires auditing all
 * sources under csrc/ before extending LAKEBASE_SUPPORTED_PG_MAJOR().
 */
#define LAKEBASE_PG16 (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000)
#define LAKEBASE_PG17 (PG_VERSION_NUM >= 170000 && PG_VERSION_NUM < 180000)
#define LAKEBASE_PG18 (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)

#define LAKEBASE_PG17_GE (PG_VERSION_NUM >= 170000)
#define LAKEBASE_PG18_GE (PG_VERSION_NUM >= 180000)

/* C forks currently support the PostgreSQL 17 major line. */
#define LAKEBASE_SUPPORTED_PG_MAJOR(version) \
    ((version) >= 170000 && (version) < 180000)

#if !LAKEBASE_SUPPORTED_PG_MAJOR(PG_VERSION_NUM)
#error "Lakebase C forks have only been ported to PostgreSQL 17"
#endif

#endif
