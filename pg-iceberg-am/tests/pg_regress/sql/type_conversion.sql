-- type_conversion.sql
-- Comprehensive test for all supported PostgreSQL to Iceberg type conversions.
-- This test covers both write (INSERT) and read (SELECT) paths for all supported types.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

SET timezone = 'UTC';

-- Attempt to disable lz4 if the environment doesn't support it
DO $$
BEGIN
    EXECUTE 'SET default_toast_compression = pglz';
EXCEPTION WHEN OTHERS THEN
    -- Ignore if the setting doesn't exist
END $$;

-- ============================================================================
-- 1. All Primitive Types Test
-- ============================================================================
-- This table includes all primitive types supported by the mapping logic.
CREATE TABLE iceberg_all_primitives (
    id integer,
    -- Numeric 
    b boolean,
    si smallint,
    i integer,
    bi bigint,
    r real,
    dp double precision,
    num_default numeric,
    num_spec numeric(12, 4),
    -- String-like
    t text,
    vc varchar(20),
    c char(10),
    n name,
    j json,
    jb jsonb,
    -- Temporal
    d date,
    tm time,
    ts timestamp,
    tstz timestamptz,
    -- Special
    u uuid,
    by bytea
) USING iceberg;

-- Insert a row with all values set
INSERT INTO iceberg_all_primitives VALUES (
    1,
    true,
    32767,
    2147483647,
    9223372036854775807,
    3.14159,
    2.718281828459,
    123456789.0123456789,
    12345678.1234,
    'text value',
    'varchar value',
    'char val',
    'name_value',
    '{"json_key": "val"}',
    '{"jsonb_key": "val"}',
    '2024-01-01',
    '12:34:56.789',
    '2024-01-01 12:34:56.789',
    '2024-01-01 12:34:56.789+00',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '\xdeadbeef'
);

-- Insert a row with all NULLs (except ID)
INSERT INTO iceberg_all_primitives (id) VALUES (2);

-- Verify all types can be read back correctly
-- Note: for json/jsonb/bytea/char we might need specific output formats to be stable in regression tests
SELECT 
    id, b, si, i, bi, r, dp, 
    num_default, num_spec,
    t, vc, c, n, 
    j::text as j, jb::text as jb,
    d, tm, ts, tstz, 
    u, 
    encode(by, 'hex') as by_hex
FROM iceberg_all_primitives ORDER BY id;

DROP TABLE iceberg_all_primitives;

-- ============================================================================
-- 2. All Supported Array (List) Types Test
-- ============================================================================
-- Only certain element types are currently supported for arrays in complex.rs
CREATE TABLE iceberg_all_arrays (
    id integer,
    b_arr boolean[],
    i2_arr smallint[],
    i4_arr integer[],
    i8_arr bigint[],
    f4_arr real[],
    f8_arr double precision[],
    t_arr text[]
) USING iceberg;

INSERT INTO iceberg_all_arrays VALUES (
    1,
    '{true, false, NULL}',
    '{1, 32767, NULL}',
    '{1, 2147483647, NULL}',
    '{1, 9223372036854775807, NULL}',
    '{1.1, 3.14, NULL}',
    '{1.111, 2.718281828, NULL}',
    '{"first", "second", NULL}'
);

-- Test empty arrays
INSERT INTO iceberg_all_arrays VALUES (
    2,
    '{}',
    '{}',
    '{}',
    '{}',
    '{}',
    '{}',
    '{}'
);

SELECT * FROM iceberg_all_arrays ORDER BY id;

DROP TABLE iceberg_all_arrays;

-- ============================================================================
-- 3. Edge Cases: Numeric Precision
-- ============================================================================
CREATE TABLE iceberg_numeric_edge (
    id integer,
    n_max numeric(38, 18),
    n_large numeric(38, 0),
    n_small numeric(38, 37)
) USING iceberg;

INSERT INTO iceberg_numeric_edge VALUES (
    1,
    12345678901234567890.123456789012345678,
    12345678901234567890123456789012345678,
    0.1234567890123456789012345678901234567
);

SELECT * FROM iceberg_numeric_edge ORDER BY id;

DROP TABLE iceberg_numeric_edge;

-- ============================================================================
-- 4. String-like Mapping Variation
-- ============================================================================
-- Test that various PG types correctly map to Iceberg String and back.
CREATE TABLE iceberg_string_mapping (
    id integer,
    v varchar(5),
    t text,
    c char(3),
    name_val name
) USING iceberg;

INSERT INTO iceberg_string_mapping VALUES (1, 'abc', 'defgh', 'igk', 'lmno');

SELECT * FROM iceberg_string_mapping ORDER BY id;

DROP TABLE iceberg_string_mapping;

-- ============================================================================
-- 5. Negative Test: Numeric Precision > 38
-- ============================================================================
-- This should fail because Iceberg only supports up to precision 38.
\set VERBOSITY terse
CREATE TABLE iceberg_numeric_fallback (
    id integer,
    n_huge numeric(40, 2)
) USING iceberg;
\set VERBOSITY default
