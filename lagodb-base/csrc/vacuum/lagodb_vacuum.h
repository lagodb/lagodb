#ifndef LAGODB_VACUUM_H
#define LAGODB_VACUUM_H

#include "postgres.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"

typedef bool (*LagodbVacuumProviderCallback)(Relation relation,
                                                VacuumParams *params,
                                                void *context);

extern bool lagodb_parse_vacuum_full(VacuumStmt *stmt,
                                       VacuumParams *params);
extern List *lagodb_expand_vacuum_relations(VacuumStmt *stmt,
                                               VacuumParams *params,
                                               MemoryContext vacuum_context);
extern Oid lagodb_relation_access_method(Oid relid);
extern void *lagodb_copy_node_to_context(const void *node,
                                            MemoryContext context);
/* -1: skipped, 0: AM changed and must be delegated, 1: provider ran,
 * 2: provider work skipped but a requested ANALYZE phase still applies. */
extern int lagodb_vacuum_provider_relation(
    VacuumRelation *vrel,
    VacuumParams *params,
    LagodbVacuumProviderCallback callback,
    void *context);

#endif
