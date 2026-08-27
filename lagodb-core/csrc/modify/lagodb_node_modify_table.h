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

typedef struct LagodbPreparedUpdateTriggerRows
{
	ItemPointerData old_tid;
	ItemPointerData new_tid;
} LagodbPreparedUpdateTriggerRows;

typedef struct LagodbModifyBridge
{
	void *state;
	bool		postgres_indexes;
	void *(*resolve_relation)(void *state, ResultRelInfo *result_rel_info);
	AttrNumber (*wholerow_attno)(void *state);
	void (*insert)(void *relation_state, TupleTableSlot *new_slot,
				   CommandId cid, int options);
	void (*preserve_trigger_row)(
		void *relation_state, TupleTableSlot *slot,
		ItemPointerData *row_id);
	void (*prepare_update_trigger_rows)(
		void *state, ResultRelInfo *source_info,
		ResultRelInfo *destination_info, TupleTableSlot *old_slot,
		TupleTableSlot *new_slot,
		LagodbPreparedUpdateTriggerRows *prepared);
	LagodbMutationResult (*update)(
		void *relation_state,
		const ItemPointerData *tuple_id,
		TupleTableSlot *old_slot, TupleTableSlot *new_slot,
		CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait);
	LagodbMutationResult (*delete_)(
		void *relation_state,
		const ItemPointerData *tuple_id,
		CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
		bool changing_partition);
} LagodbModifyBridge;

extern TupleTableSlot *lagodb_exec_modify_table(
	ModifyTableState *mtstate, LagodbModifyBridge *bridge);
