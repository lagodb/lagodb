/*-------------------------------------------------------------------------
 *
 * lakebase_analyze.c
 *      Narrow PostgreSQL ANALYZE ReadStream adapter.
 *
 * PostgreSQL keeps struct ReadStream private to read_stream.c. ANALYZE stores
 * its stack-owned BlockSamplerData in callback_private_data before invoking
 * the table-AM scan callbacks. This adapter mirrors the audited private layout
 * so an extension table AM can obtain the exact targrows (or inherited
 * childtargrows) selected by analyze.c.
 *
 * Keep PostgreSQL-version differences in this file behind
 * lakebase_pg_compat.h. Rust callers validate the copied sampler state against
 * tickets consumed from the same ReadStream; upstream hashes and the PG17
 * minor CI matrix audit the private-layout dereference.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/read_stream.h"
#include "utils/sampling.h"

#include "lakebase_pg_compat.h"
#include "lakebase_analyze.h"

#if LAKEBASE_PG17

/* Copied from PostgreSQL 17.10 src/backend/storage/aio/read_stream.c. */
typedef struct InProgressIO
{
    int16 buffer_index;
    ReadBuffersOperation op;
} InProgressIO;

/*
 * Complete the opaque public declaration with PostgreSQL 17's private layout.
 * PG17.5 added io_combine_limit before the fields used by this bridge, so that
 * layout epoch is selected locally. Keeping the complete definition lets the
 * C compiler apply the same alignment rules as PostgreSQL.
 */
struct ReadStream
{
    int16 max_ios;
#if PG_VERSION_NUM >= 170005
    /* Added in PostgreSQL 17.5. */
    int16 io_combine_limit;
#endif
    int16 ios_in_progress;
    int16 queue_size;
    int16 max_pinned_buffers;
    int16 pinned_buffers;
    int16 distance;
    bool advice_enabled;

    BlockNumber buffered_blocknum;

    ReadStreamBlockNumberCB callback;
    void *callback_private_data;

    BlockNumber seq_blocknum;

    BlockNumber pending_read_blocknum;
    int16 pending_read_nblocks;

    size_t per_buffer_data_size;
    void *per_buffer_data;

    InProgressIO *ios;
    int16 oldest_io_index;
    int16 next_io_index;

    bool fast_path;

    int16 oldest_buffer_index;
    int16 next_buffer_index;
    Buffer buffers[FLEXIBLE_ARRAY_MEMBER];
};

#else
#error "ANALYZE ReadStream bridge has no audited layout for this PostgreSQL major version"
#endif

bool
lakebase_read_stream_analyze_sampler_state(
    ReadStream *stream,
    LakebaseAnalyzeSamplerState *state)
{
    BlockSamplerData *sampler;

    if (stream == NULL || state == NULL || stream->callback == NULL ||
        stream->callback_private_data == NULL)
        return false;

    /*
     * analyze.c's block_sampling_read_stream_next() installs &bs here and
     * keeps it live until read_stream_end(). This adapter is called only from
     * scan_analyze_next_block, while that acquire_sample_rows() frame is live.
     */
    sampler = (BlockSamplerData *) stream->callback_private_data;
    if (sampler->N == InvalidBlockNumber || sampler->n <= 0 ||
        sampler->m < 0 || sampler->m > sampler->n ||
        (BlockNumber) sampler->m > sampler->t || sampler->t > sampler->N)
        return false;

    state->population_blocks = sampler->N;
    state->target_rows = sampler->n;
    state->visited_blocks = sampler->t;
    state->selected_blocks = sampler->m;
    return true;
}
