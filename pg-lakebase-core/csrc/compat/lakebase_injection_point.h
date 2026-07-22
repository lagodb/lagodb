#ifndef LAKEBASE_INJECTION_POINT_H
#define LAKEBASE_INJECTION_POINT_H

#include "lakebase_pg_compat.h"

/*
 * PostgreSQL 18 added a runtime-data argument to INJECTION_POINT(). Older
 * server versions have no native injection-point facility and compile the
 * call site to a no-op. The shared compatibility boundary still rejects
 * PG18 until every Lakebase C fork has completed its PG18 audit.
 */
#if LAKEBASE_PG18
#include "utils/injection_point.h"
#define LAKEBASE_INJECTION_POINT(name) INJECTION_POINT((name), NULL)
#elif LAKEBASE_PG17
#include "utils/injection_point.h"
#define LAKEBASE_INJECTION_POINT(name) INJECTION_POINT((name))
#elif PG_VERSION_NUM < 170000
#define LAKEBASE_INJECTION_POINT(name) ((void) (name))
#else
#error "Lakebase injection points have not been audited for this PostgreSQL major version"
#endif

extern void lakebase_injection_point_run(const char *name);

#endif
