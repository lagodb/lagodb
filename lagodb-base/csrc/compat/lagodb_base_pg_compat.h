#ifndef LAGODB_BASE_PG_COMPAT_H
#define LAGODB_BASE_PG_COMPAT_H

#include "postgres.h"

/*
 * PostgreSQL support boundary for lagodb-base's runtime C bridges.
 * Base owns this gate independently of core so either crate can be extracted.
 * Audit every base C bridge before enabling another major here and in build.rs.
 */
#define LAGODB_BASE_SUPPORTED_PG_MAJOR(version) \
    ((version) >= 170000 && (version) < 180000)

#if !LAGODB_BASE_SUPPORTED_PG_MAJOR(PG_VERSION_NUM)
#error "LagoDB base C bridges have only been ported to PostgreSQL 17"
#endif

#endif
