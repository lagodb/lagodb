#pragma once

#include "postgres.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"

typedef enum LakebaseMutationOutcome
{
	LAKEBASE_MUTATION_APPLIED = 0,
	LAKEBASE_MUTATION_SELF_MODIFIED = 1,
	LAKEBASE_MUTATION_DELETED = 2
} LakebaseMutationOutcome;

typedef struct LakebaseMutationResult
{
	LakebaseMutationOutcome outcome;
	CommandId	modifying_cid;
} LakebaseMutationResult;

typedef struct LakebasePreparedUpdateTriggerRows
{
	ItemPointerData old_tid;
	ItemPointerData new_tid;
} LakebasePreparedUpdateTriggerRows;

typedef struct LakebaseModifyBridge
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
		LakebasePreparedUpdateTriggerRows *prepared);
	LakebaseMutationResult (*update)(
		void *relation_state,
		const ItemPointerData *tuple_id,
		TupleTableSlot *old_slot, TupleTableSlot *new_slot,
		CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait);
	LakebaseMutationResult (*delete_)(
		void *relation_state,
		const ItemPointerData *tuple_id,
		CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
		bool changing_partition);
} LakebaseModifyBridge;

extern TupleTableSlot *lakebase_exec_modify_table(
	ModifyTableState *mtstate, LakebaseModifyBridge *bridge);
