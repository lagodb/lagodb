# Mutation executor support

This core module holds the storage-neutral ModifyTable contracts and the
core-owned trigger-row store (`trigger_rows`). The PostgreSQL executor
callbacks that drive them live in `table_am::callbacks::mutation`.

`AmModifyQueryState` owns provider-defined query handles shared by all
ModifyTable nodes in one PostgreSQL `EState`. The lifetime of the physical
identity namespace remains a provider decision; Iceberg keeps its file
registry at transaction/relation scope. `AmModifyState::begin_modify` constructs
the complete relation-local writer session; the returned state exposes mutation,
finish, and abort methods.
`ModifyStateContext<Q, C>` carries the shared query-state handle, PostgreSQL
command, and either an independent-write marker or the provider context
captured by a target scan. Core does not define snapshot IDs, predicates, file
paths, or row positions.

`ModifyScanBinding<Q>` owns a clone of the typed query-state handle plus the
target relation OID. A provider's Modify scan uses it to register storage
identity sources in the correct relation namespace. Registration occurs at
file/run boundaries; per-row carrier encoding is the AM's direct static call.
The high `ItemPointer` block bit is reserved for core trigger rows. The binding
has no raw pointer back to `ResultRelationState`.

Provider-specific relation execution belongs in the provider crate. For
Iceberg this includes scan context, synthetic physical-`ctid` codec,
transaction/relation file interning, shared mutation-owner bitmaps, modify
state, and conflict-validation metadata.

Core retains complete rows only for PostgreSQL AFTER ROW trigger execution.
Those rows use one query-level PostgreSQL tuplestore per relation (spillable
under `work_mem`) plus bounded OLD/NEW read slots. A core-owned, backend-global
temporary ID routes nested queries exactly; a missing temporary ID never falls
back to provider physical fetch. Core does not cache complete provider rows in
a Rust map or put storage-specific state into the trigger adapter.
Deferrable AFTER ROW triggers are rejected because their lifetime exceeds the
query-local tuplestore, matching PostgreSQL's FDW restriction.

COPY FROM bypasses ModifyTable and uses its separate utility-scoped
`AmCopySession` lifecycle.

`RelationTriggerPolicy` (in `table_am::callbacks::mutation`) owns the shared
execution-time capability checks.
ModifyTable checks each provider relation during result-state initialization;
COPY FROM checks its locked relation during preparation, before opening a
source or starting the COPY driver. ModifyTable and COPY targets retain the
deferred-trigger restriction and the immediate-trigger row store. No relation
kind or TableAM pointer is changed.
