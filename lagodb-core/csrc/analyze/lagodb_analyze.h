#ifndef LAGODB_ANALYZE_H
#define LAGODB_ANALYZE_H

#include "postgres.h"
#include "storage/read_stream.h"

/*
 * Snapshot of the BlockSamplerData owned by PostgreSQL's
 * acquire_sample_rows(). Values are copied while its stack frame and the
 * ReadStream callback-private pointer are both live.
 */
typedef struct LagodbAnalyzeSamplerState
{
    BlockNumber population_blocks;
    int target_rows;
    BlockNumber visited_blocks;
    int selected_blocks;
} LagodbAnalyzeSamplerState;

extern bool lagodb_read_stream_analyze_sampler_state(
    ReadStream *stream,
    LagodbAnalyzeSamplerState *state);

#endif
