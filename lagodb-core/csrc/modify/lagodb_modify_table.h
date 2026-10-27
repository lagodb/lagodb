#pragma once

#include "postgres.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"

typedef enum LagodbMutationOutcome
{
	LAGODB_MUTATION_APPLIED = 0,
	LAGODB_MUTATION_SELF_MODIFIED = 1,
	LAGODB_MUTATION_DELETED = 2
} LagodbMutationOutcome;

typedef struct LagodbMutationResult
{
	LagodbMutationOutcome outcome;
	CommandId	modifying_cid;
} LagodbMutationResult;

typedef struct LagodbModifyBridge
{
	void	   *state;
	bool		postgres_indexes;
	void	   *(*resolve_relation) (void *state, ResultRelInfo *result_rel_info);
	AttrNumber	wholerow_attno;
	void		(*insert) (void *relation_state, TupleTableSlot *new_slot,
						   CommandId cid, int options);
	void		(*preserve_trigger_row) (
										 void *relation_state, TupleTableSlot *slot,
										 ItemPointerData *row_id);
	LagodbMutationResult (*update) (
									void *relation_state,
									const ItemPointerData *tuple_id,
									TupleTableSlot *old_slot, TupleTableSlot *new_slot,
									CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait);
	LagodbMutationResult (*delete_) (
									 void *relation_state,
									 const ItemPointerData *tuple_id,
									 CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait);
} LagodbModifyBridge;

/* Node-specific fork entry points, owned by the lifecycle adapter below. */
extern ModifyTableState *lagodb_init_modify_table_state(
														ModifyTableState *mtstate, ModifyTable *plan, EState *estate, int eflags);
extern TupleTableSlot *lagodb_exec_modify_table_with_bridge(
															ModifyTableState *mtstate, LagodbModifyBridge *bridge);
extern void LagodbExecEndModifyTable(ModifyTableState *mtstate);
extern void LagodbExecReScanModifyTable(ModifyTableState *mtstate);

/* Complete lifecycle for nodes initialized by this adapter. */
extern ModifyTableState *lagodb_exec_init_modify_table(
													   ModifyTable *plan, EState *estate, int eflags);
extern TupleTableSlot *lagodb_exec_modify_table(
												ModifyTableState *mtstate, LagodbModifyBridge *bridge);
extern void lagodb_exec_end_modify_table(ModifyTableState *mtstate);
extern void lagodb_exec_rescan_modify_table(ModifyTableState *mtstate);
