#ifndef LAKEBASE_VACUUM_FULL_H
#define LAKEBASE_VACUUM_FULL_H

#include "postgres.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"

typedef bool (*LakebaseVacuumProviderCallback)(Relation relation,
                                                VacuumParams *params,
                                                void *context);

extern bool lakebase_parse_vacuum_full(VacuumStmt *stmt,
                                       VacuumParams *params);
extern List *lakebase_expand_vacuum_relations(VacuumStmt *stmt,
                                               VacuumParams *params,
                                               MemoryContext vacuum_context);
extern Oid lakebase_relation_access_method(Oid relid);
extern void *lakebase_copy_node_to_context(const void *node,
                                            MemoryContext context);
/* -1: skipped, 0: AM changed and must be delegated, 1: provider ran,
 * 2: provider work skipped but a requested ANALYZE phase still applies. */
extern int lakebase_vacuum_provider_relation(
    VacuumRelation *vrel,
    VacuumParams *params,
    LakebaseVacuumProviderCallback callback,
    void *context);

#endif
