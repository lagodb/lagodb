#ifndef LAGODB_VACUUM_PROBE_H
#define LAGODB_VACUUM_PROBE_H

#include "postgres.h"
#include "nodes/parsenodes.h"

typedef bool (*LagodbVacuumProbeCallback) (Oid access_method);

/* Table AM of an unlocked relation, or InvalidOid if it no longer exists. */
extern Oid	lagodb_vacuum_relation_am(Oid relid);

/* Called after command preparation, before any target lock is taken. */
extern bool lagodb_vacuum_probe(
								VacuumStmt *stmt,
								LagodbVacuumProbeCallback callback);

#endif
