#ifndef LAGODB_INJECTION_POINT_H
#define LAGODB_INJECTION_POINT_H

#include "lagodb_pg_compat.h"

/*
 * PostgreSQL 17 introduced injection points and PostgreSQL 18 added a
 * runtime-data argument to INJECTION_POINT(). PostgreSQL 16 has no such
 * facility, so its builds keep this adapter as a no-op.
 */
#if LAGODB_PG18_GE
#include "utils/injection_point.h"
#define LAGODB_INJECTION_POINT(name) INJECTION_POINT((name), NULL)
#elif LAGODB_PG17_GE
#include "utils/injection_point.h"
#define LAGODB_INJECTION_POINT(name) INJECTION_POINT((name))
#else
#define LAGODB_INJECTION_POINT(name) ((void) (name))
#endif

extern void lagodb_injection_point_run(const char *name);

#endif
