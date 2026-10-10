#ifndef LAGODB_COPY_WRITE_H
#define LAGODB_COPY_WRITE_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"

/* Borrowed for one synchronous executor call; the Rust driver owns all handles. */
typedef struct LagodbCopyWriteBridge
{
    void *context;
    void *(*bind) (void *context, Relation relation, CommandId cid);
    void (*insert) (void *binding, TupleTableSlot *slot, int options);
    void (*insert_batch) (void *binding, TupleTableSlot **slots, int count, int options);
    void (*finish_bulk) (void *binding, int options);
    void (*publish) (void *context);
} LagodbCopyWriteBridge;

#endif
