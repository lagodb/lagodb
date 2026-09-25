/*
 * Complete lifecycle for an embedded PostgreSQL ModifyTable fork.
 *
 * ExecInitNode/ExecEndNode dispatch ModifyTable to the server's implementation.
 * Keep their common contracts here while selecting the extension's fork.
 * Initialization and teardown follow execProcnode.c from PostgreSQL 17.10;
 * execution uses the backend's ExecSetExecProcNode/ExecProcNode wrappers.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodeSubplan.h"
#include "miscadmin.h"

#include "lagodb_modify_table.h"

typedef struct LagodbModifyTableExecutor
{
	/* The PG node prefix stays at the same address throughout its lifecycle. */
	ModifyTableState state;
	/* Borrowed from Rust only while lagodb_exec_modify_table is executing. */
	LagodbModifyBridge *bridge;
} LagodbModifyTableExecutor;

static TupleTableSlot *
LagodbModifyTableNext(PlanState *state)
{
	LagodbModifyTableExecutor *executor = (LagodbModifyTableExecutor *) state;

	return lagodb_exec_modify_table_with_bridge(&executor->state,
												executor->bridge);
}

ModifyTableState *
lagodb_exec_init_modify_table(ModifyTable *plan, EState *estate, int eflags,
							  bool provider_owns_partitioned_table)
{
	LagodbModifyTableExecutor *executor;
	PlanState  *state;
	List	   *subplans = NIL;
	ListCell   *cell;

	check_stack_depth();
	executor = palloc0(sizeof(LagodbModifyTableExecutor));
	NodeSetTag(&executor->state, T_ModifyTableState);
	lagodb_init_modify_table_state(&executor->state, plan, estate, eflags,
								   provider_owns_partitioned_table);
	state = &executor->state.ps;

	/*
	 * PostgreSQL ExecInitNode's common tail, after node-specific
	 * initialization.
	 */
	ExecSetExecProcNode(state, LagodbModifyTableNext);
	foreach(cell, plan->plan.initPlan)
	{
		SubPlan    *subplan = (SubPlan *) lfirst(cell);
		SubPlanState *substate;

		Assert(IsA(subplan, SubPlan));
		substate = ExecInitSubPlan(subplan, state);
		subplans = lappend(subplans, substate);
	}
	state->initPlan = subplans;
	if (estate->es_instrument)
		state->instrument = InstrAlloc(1, estate->es_instrument,
									   state->async_capable);

	return &executor->state;
}

TupleTableSlot *
lagodb_exec_modify_table(ModifyTableState *state, LagodbModifyBridge *bridge)
{
	LagodbModifyTableExecutor *executor = (LagodbModifyTableExecutor *) state;
	TupleTableSlot *result;

	/*
	 * An ERROR abandons this executor; teardown never uses the borrowed
	 * bridge.
	 */
	executor->bridge = bridge;
	result = ExecProcNode(&state->ps);
	executor->bridge = NULL;
	return result;
}

void
lagodb_exec_end_modify_table(ModifyTableState *state)
{
	/* PostgreSQL ExecEndNode's common prefix, followed by the fork's cleanup. */
	check_stack_depth();
	if (state->ps.chgParam != NULL)
	{
		bms_free(state->ps.chgParam);
		state->ps.chgParam = NULL;
	}
	LagodbExecEndModifyTable(state);
}

void
lagodb_exec_rescan_modify_table(ModifyTableState *state)
{
	/* Both PostgreSQL and the fork reject ModifyTable rescans. */
	LagodbExecReScanModifyTable(state);
}
