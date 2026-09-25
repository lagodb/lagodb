#ifndef LAGODB_MAINTENANCE_H
#define LAGODB_MAINTENANCE_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"

#include "lagodb_base_pg_compat.h"

/* Must match Rust's repr(C) ProcessUtilityArgs. Borrowed for one invocation. */
typedef struct LagodbMaintenanceUtilityArgs
{
	PlannedStmt *pstmt;
	const char *query_string;
	bool		read_only_tree;
	ProcessUtilityContext context;
	ParamListInfo params;
	QueryEnvironment *query_env;
	DestReceiver *dest;
	QueryCompletion *completion_tag;
} LagodbMaintenanceUtilityArgs;

typedef bool (*LagodbMaintenanceRouteCallback) (
												const LagodbMaintenanceUtilityArgs *args);

extern void lagodb_check_maintenance_recursion(VacuumStmt *stmt);
extern bool lagodb_execute_maintenance_command(
											   const LagodbMaintenanceUtilityArgs *args,
											   ProcessUtility_hook_type previous,
											   LagodbMaintenanceRouteCallback route);

#endif
