-- Text EXPLAIN ANALYZE with PostgreSQL's regression options. PostgreSQL row
-- counts are masked as in partition_prune.sql because their text format
-- differs across major versions. LagoDB worker peak memory depends on how
-- producer batches interleave in each worker, so it is masked as
-- select_parallel.sql masks parallel sort memory. A caller can opt into the
-- same treatment for a leader network-boundary peak while retaining the
-- invariant that the peak is nonzero. Other LagoDB metrics are left unchanged.
-- LagoDB duration properties ("N ms") are masked as explain.sql masks them,
-- but PostgreSQL's per-node actual time is not, so callers that request timing
-- select only the properties they assert.
CREATE FUNCTION pg_temp.explain_analyze_text(
    query text,
    verbose_plan boolean DEFAULT false,
    timing boolean DEFAULT false,
    mask_local_memory boolean DEFAULT false
) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
    ln text;
BEGIN
    FOR ln IN
        EXECUTE format(
            'EXPLAIN (ANALYZE, VERBOSE %s, COSTS OFF, TIMING %s, SUMMARY OFF, BUFFERS OFF) %s',
            verbose_plan::text, timing::text, query)
    LOOP
        ln := regexp_replace(ln, 'actual rows=\d+(?:\.\d+)? loops=\d+',
                             'actual rows=N loops=N');
        ln := regexp_replace(ln, '(Worker \w+ Peak Memory Bytes): \d+', '\1: N');
        IF mask_local_memory THEN
            ln := regexp_replace(
                ln,
                '(Local Engine Peak Memory Bytes): [1-9][0-9]*',
                '\1: N');
        END IF;
        ln := regexp_replace(ln, '\d+\.\d+ ms', 'N ms', 'g');
        RETURN NEXT ln;
    END LOOP;
END;
$$;
