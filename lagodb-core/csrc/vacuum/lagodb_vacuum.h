#ifndef LAGODB_VACUUM_H
#define LAGODB_VACUUM_H

#include "postgres.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"
#include "storage/buf.h"
#include "utils/queryenvironment.h"

typedef bool (*LagodbVacuumRouteCallback) (Relation relation,
										   VacuumParams *params);
typedef void (*LagodbVacuumProviderCallback) (Relation relation,
											  VacuumParams *params,
											  void *context);

/* Parse and validate every PostgreSQL VACUUM/ANALYZE option into params and ring size. */
extern void lagodb_parse_vacuum_options(VacuumStmt *stmt,
										const char *query_string,
										QueryEnvironment *query_env,
										VacuumParams *params,
										int *ring_size_kb);
extern BufferAccessStrategy lagodb_make_vacuum_buffer_strategy(
															   int ring_size_kb,
															   MemoryContext context);
extern void lagodb_initialize_maintenance_costs(void);
extern void lagodb_finish_maintenance_costs(void);
extern void lagodb_check_maintenance_command_state(VacuumStmt *stmt);
extern List *lagodb_expand_vacuum_relations(VacuumStmt *stmt,
											VacuumParams *params,
											MemoryContext vacuum_context);

/*
 * Execute one PostgreSQL vacuum_rel() state machine. Provider routing is resolved
 * again from the live Relation after the execution lock is acquired. At entry
 * and exit no transaction is active. The runtime command scope owns recursion
 * protection across this call and any subsequent ANALYZE phase.
 */
extern bool lagodb_vacuum_relation(
								   VacuumRelation *vrel,
								   VacuumParams *params,
								   BufferAccessStrategy bstrategy,
								   LagodbVacuumRouteCallback route_callback,
								   LagodbVacuumProviderCallback provider_callback,
								   void *context);

#endif
