#ifndef LAGODB_VACUUM_PROBE_H
#define LAGODB_VACUUM_PROBE_H

#include "postgres.h"
#include "nodes/parsenodes.h"

typedef bool (*LagodbVacuumProbeCallback) (Oid access_method,
										   char relkind,
										   bits32 options);

/* Called after command preparation with its validated VacuumParams options. */
extern bool lagodb_vacuum_probe(
								VacuumStmt *stmt,
								bits32 options,
								LagodbVacuumProbeCallback callback);

#endif
