#ifndef LAGODB_VACUUM_H
#define LAGODB_VACUUM_H

#include "postgres.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"
#include "utils/queryenvironment.h"

typedef void (*LagodbVacuumProviderCallback) (Relation relation,
											  VacuumParams *params,
											  void *context);

/* Report the FULL option without validating the rest of the statement. */
extern bool lagodb_vacuum_requests_full(VacuumStmt *stmt);

/* Parse and validate every PostgreSQL VACUUM option into params. */
extern void lagodb_parse_vacuum_options(VacuumStmt *stmt,
										const char *query_string,
										QueryEnvironment *query_env,
										VacuumParams *params);
extern void lagodb_check_maintenance_command_state(void);
extern List *lagodb_expand_vacuum_relations(VacuumStmt *stmt,
											VacuumParams *params,
											MemoryContext vacuum_context);

/* Build the VACUUM statement that PostgreSQL runs for native relations. */
extern VacuumStmt *lagodb_make_vacuum_stmt(VacuumStmt *stmt, List *relations,
										   MemoryContext context);

/* Build the ANALYZE statement that PostgreSQL runs for provider relations. */
extern VacuumStmt *lagodb_make_analyze_stmt(VacuumStmt *stmt, List *relations,
											MemoryContext context);

/*
 * Rewrite one provider relation under its VACUUM FULL execution lock. At entry
 * and exit no transaction is active.
 */
extern bool lagodb_vacuum_full_relation(VacuumRelation *vrel,
										VacuumParams *params,
										LagodbVacuumProviderCallback provider_callback,
										void *context);

#endif
