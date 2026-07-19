/*-------------------------------------------------------------------------
 *
 * read_stream_pg17.c
 *      Narrow PostgreSQL 17 ANALYZE ReadStream adapter.
 *
 * PostgreSQL keeps struct ReadStream private to read_stream.c.  ANALYZE
 * stores its stack-owned BlockSamplerData in callback_private_data before it
 * invokes the table-AM scan callbacks.  This PG17-versioned adapter mirrors
 * the private ReadStream layout so an extension table AM can obtain the exact
 * targrows (or inherited childtargrows) selected by analyze.c.
 *
 * Re-audit this file against the upstream files and hashes recorded in
 * PG17.md before enabling a different PostgreSQL minor version.  Rust callers
 * must also validate the copied sampler state against the tickets consumed
 * from the same ReadStream.  Exact build/runtime version guards protect the
 * private-layout dereference; sampler invariants protect its semantics.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/read_stream.h"
#include "utils/guc.h"
#include "utils/sampling.h"

#include "read_stream_pg17.h"

#if PG_VERSION_NUM != 170010
#error "lakebase ANALYZE ReadStream adapter requires its audited PostgreSQL 17.10 layout"
#endif

/* Copied from PostgreSQL 17.10 src/backend/storage/aio/read_stream.c. */
typedef struct InProgressIO
{
    int16 buffer_index;
    ReadBuffersOperation op;
} InProgressIO;

/*
 * Complete the opaque public declaration with PostgreSQL 17.10's private
 * layout.  Keeping the complete definition (rather than a hand-computed byte
 * offset) lets the C compiler apply the same alignment rules as PostgreSQL.
 */
struct ReadStream
{
    int16 max_ios;
    int16 io_combine_limit;
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

bool
lakebase_read_stream_analyze_sampler_state(
    ReadStream *stream,
    LakebaseAnalyzeSamplerState *state)
{
    BlockSamplerData *sampler;
    const char *runtime_version;

    /*
     * PG_MODULE_MAGIC checks only the major version.  Refuse to dereference
     * the private layout if a binary built with 17.10 headers was copied into
     * a backend running another PG17 minor release.
     */
    runtime_version = GetConfigOption("server_version_num", false, false);
    if (runtime_version == NULL || strcmp(runtime_version, "170010") != 0)
        return false;

    if (stream == NULL || state == NULL || stream->callback == NULL ||
        stream->callback_private_data == NULL)
        return false;

    /*
     * analyze.c's block_sampling_read_stream_next() installs &bs here and
     * keeps it live until read_stream_end().  This adapter is called only from
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
