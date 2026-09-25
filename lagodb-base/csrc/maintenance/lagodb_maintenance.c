/*
 * One backend-local lifecycle for routed VACUUM and ANALYZE commands.
 *
 * Both provider execution and native parent delegation must hold this state.
 * A native ANALYZE can invoke a type/index function that submits provider-root
 * ANALYZE; consuming that inner statement bypasses PostgreSQL vacuum()'s private
 * in_vacuum check. Relation-local flags cannot detect the native outer command.
 *
 * The parent hook is called directly from C so its ERROR reaches PG_FINALLY
 * without jumping over a Rust routing frame. The Rust route callback must use
 * pg_guard, and the Rust caller must guard this C entry point. The borrowed
 * arguments live on the caller's stack across maintenance transaction commits.
 *
 * LagoDB pre/post hooks run outside this scope. Unclaimed statements retain
 * the captured parent chain; consumption remains the route callback's decision.
 *
 * NOTE: Once the runtime ProcessUtility hook is installed, this scope also
 * covers native-only VACUUM/ANALYZE, even without registered providers. It is
 * wider than PostgreSQL vacuum()'s private in_vacuum interval: the captured parent's
 * entire call is inside it, so VACUUM/ANALYZE submitted by that hook's own
 * before/after logic is rejected until it returns. Recursion is checked at
 * hook entry, before native transaction and option validation. Parent-chain
 * delegation is preserved, but native maintenance behavior is not identical
 * to PostgreSQL without this runtime hook.
 *
 * This guard directly rejects only VACUUM/ANALYZE (VacuumStmt). Other utility
 * commands remain allowed, but maintenance they submit within this scope is
 * rejected; an unhandled rejection also fails the calling operation. Sequential
 * maintenance commands run after the preceding scope has ended. Native
 * maintenance entered directly by the kernel without ProcessUtility is not
 * covered by this scope.
 */
#include "postgres.h"

#include "lagodb_maintenance.h"

static bool maintenance_active = false;

void
lagodb_check_maintenance_recursion(VacuumStmt *stmt)
{
	if (maintenance_active)
	{
		const char *command = stmt->is_vacuumcmd ? "VACUUM" : "ANALYZE";

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s cannot be executed from VACUUM or ANALYZE", command)));
	}
}

bool
lagodb_execute_maintenance_command(const LagodbMaintenanceUtilityArgs *args,
								   ProcessUtility_hook_type previous,
								   LagodbMaintenanceRouteCallback route)
{
	bool		consumed;

	/* A rejected inner command must not clear the outer command's state. */
	lagodb_check_maintenance_recursion((VacuumStmt *) args->pstmt->utilityStmt);

	PG_TRY();
	{
		maintenance_active = true;
		consumed = route(args);
		if (!consumed)
		{
			if (previous != NULL)
				previous(args->pstmt, args->query_string, args->read_only_tree,
						 args->context, args->params, args->query_env,
						 args->dest, args->completion_tag);
			else
				standard_ProcessUtility(args->pstmt, args->query_string,
										args->read_only_tree, args->context,
										args->params, args->query_env,
										args->dest, args->completion_tag);
		}
	}
	PG_FINALLY();
	{
		maintenance_active = false;
	}
	PG_END_TRY();

	return consumed;
}
