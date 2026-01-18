-- table_insert_select.sql
-- Test INSERT and SELECT operations for Iceberg tables on local filesystem.
-- This test verifies that data can be inserted into Iceberg tables and
-- correctly retrieved via SELECT queries.

-- Clean slate: drop and recreate extension
DROP EXTENSION IF EXISTS pg_am_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_am_iceberg;

-- ============================================================================
-- Test 1: Basic integer column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_int_test (
    id integer
) USING iceberg;

-- Insert single row
INSERT INTO iceberg_int_test VALUES (1);
INSERT INTO iceberg_int_test VALUES (2);
INSERT INTO iceberg_int_test VALUES (3);

-- Verify SELECT returns inserted data
SELECT * FROM iceberg_int_test ORDER BY id;

-- Verify COUNT works
SELECT COUNT(*) AS row_count FROM iceberg_int_test;

DROP TABLE iceberg_int_test;

-- ============================================================================
-- Test 2: Multiple columns with different integer types
-- ============================================================================
CREATE TABLE iceberg_multi_int_test (
    id integer,
    small_val smallint,
    big_val bigint
) USING iceberg;

INSERT INTO iceberg_multi_int_test VALUES (1, 100, 1000000000000);
INSERT INTO iceberg_multi_int_test VALUES (2, 200, 2000000000000);

SELECT * FROM iceberg_multi_int_test ORDER BY id;

DROP TABLE iceberg_multi_int_test;

-- ============================================================================
-- Test 3: Text/String column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_text_test (
    id integer,
    name text
) USING iceberg;

INSERT INTO iceberg_text_test VALUES (1, 'Alice');
INSERT INTO iceberg_text_test VALUES (2, 'Bob');
INSERT INTO iceberg_text_test VALUES (3, 'Charlie');

SELECT * FROM iceberg_text_test ORDER BY id;

-- Verify text query
SELECT name FROM iceberg_text_test WHERE id = 2;

DROP TABLE iceberg_text_test;

-- ============================================================================
-- Test 4: Boolean column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_bool_test (
    id integer,
    active boolean
) USING iceberg;

INSERT INTO iceberg_bool_test VALUES (1, true);
INSERT INTO iceberg_bool_test VALUES (2, false);
INSERT INTO iceberg_bool_test VALUES (3, true);

SELECT * FROM iceberg_bool_test ORDER BY id;

-- Verify boolean filter
SELECT id FROM iceberg_bool_test WHERE active = true ORDER BY id;

DROP TABLE iceberg_bool_test;

-- ============================================================================
-- Test 5: Float/Double column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_float_test (
    id integer,
    float_val real,
    double_val double precision
) USING iceberg;

INSERT INTO iceberg_float_test VALUES (1, 3.14, 2.718281828);
INSERT INTO iceberg_float_test VALUES (2, 1.5, 9.99);

SELECT * FROM iceberg_float_test ORDER BY id;

DROP TABLE iceberg_float_test;

-- ============================================================================
-- Test 6: NULL values INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_null_test (
    id integer,
    name text,
    value integer
) USING iceberg;

INSERT INTO iceberg_null_test VALUES (1, 'Valid', 100);
INSERT INTO iceberg_null_test VALUES (2, NULL, 200);
INSERT INTO iceberg_null_test VALUES (3, 'Another', NULL);
INSERT INTO iceberg_null_test VALUES (4, NULL, NULL);

SELECT * FROM iceberg_null_test ORDER BY id;

-- Verify NULL filtering
SELECT id FROM iceberg_null_test WHERE name IS NULL ORDER BY id;
SELECT id FROM iceberg_null_test WHERE value IS NOT NULL ORDER BY id;

DROP TABLE iceberg_null_test;

-- ============================================================================
-- Test 7: Multi-row INSERT (single statement)
-- ============================================================================
CREATE TABLE iceberg_multi_insert_test (
    id integer,
    name text
) USING iceberg;

-- Insert multiple rows in a single statement
INSERT INTO iceberg_multi_insert_test VALUES 
    (1, 'Row1'),
    (2, 'Row2'),
    (3, 'Row3'),
    (4, 'Row4'),
    (5, 'Row5');

SELECT * FROM iceberg_multi_insert_test ORDER BY id;
SELECT COUNT(*) AS row_count FROM iceberg_multi_insert_test;

DROP TABLE iceberg_multi_insert_test;

-- ============================================================================
-- Test 8: Mixed types comprehensive test
-- ============================================================================
CREATE TABLE iceberg_mixed_test (
    id integer,
    name text,
    active boolean,
    score double precision
) USING iceberg;

INSERT INTO iceberg_mixed_test VALUES 
    (1, 'Alice', true, 95.5),
    (2, 'Bob', false, 82.3),
    (3, 'Charlie', true, 91.0);

SELECT * FROM iceberg_mixed_test ORDER BY id;

-- Verify various queries
SELECT name, score FROM iceberg_mixed_test WHERE active = true ORDER BY score DESC;
SELECT COUNT(*) AS active_count FROM iceberg_mixed_test WHERE active = true;

DROP TABLE iceberg_mixed_test;

-- ============================================================================
-- Test 9: Large batch INSERT
-- ============================================================================
CREATE TABLE iceberg_batch_test (
    id integer,
    value integer
) USING iceberg;

-- Insert a larger batch of data
INSERT INTO iceberg_batch_test
SELECT g, g * 10
FROM generate_series(1, 100) AS g;

-- Verify row count
SELECT COUNT(*) AS total_rows FROM iceberg_batch_test;

-- Verify some sample values
SELECT * FROM iceberg_batch_test WHERE id IN (1, 50, 100) ORDER BY id;

-- Verify aggregation
SELECT SUM(value) AS sum_value FROM iceberg_batch_test;
SELECT AVG(value)::integer AS avg_value FROM iceberg_batch_test;

DROP TABLE iceberg_batch_test;

-- ============================================================================
-- Test 10: Timestamp column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_timestamp_test (
    id integer,
    created_at timestamp,
    updated_at timestamp
) USING iceberg;

INSERT INTO iceberg_timestamp_test VALUES
    (1, '2024-01-15 10:30:00', '2024-01-15 11:00:00'),
    (2, '2024-06-20 14:45:30', '2024-06-20 15:00:00'),
    (3, '2024-12-31 23:59:59', '2025-01-01 00:00:00');

SELECT * FROM iceberg_timestamp_test ORDER BY id;

-- Verify timestamp filtering
SELECT id FROM iceberg_timestamp_test WHERE created_at > '2024-06-01' ORDER BY id;

DROP TABLE iceberg_timestamp_test;

-- ============================================================================
-- Test 11: Timestamp with time zone column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_timestamptz_test (
    id integer,
    event_time timestamptz
) USING iceberg;

INSERT INTO iceberg_timestamptz_test VALUES
    (1, '2024-01-15 10:30:00+00'),
    (2, '2024-06-20 14:45:30+08'),
    (3, '2024-12-31 23:59:59-05');

SELECT * FROM iceberg_timestamptz_test ORDER BY id;

DROP TABLE iceberg_timestamptz_test;

-- ============================================================================
-- Test 12: Date and Time columns INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_date_time_test (
    id integer,
    birth_date date,
    start_time time
) USING iceberg;

INSERT INTO iceberg_date_time_test VALUES
    (1, '1990-05-15', '09:00:00'),
    (2, '1985-12-25', '14:30:00'),
    (3, '2000-01-01', '23:59:59');

SELECT * FROM iceberg_date_time_test ORDER BY id;

-- Verify date filtering
SELECT id, birth_date FROM iceberg_date_time_test WHERE birth_date > '1990-01-01' ORDER BY id;

DROP TABLE iceberg_date_time_test;

-- ============================================================================
-- Test 13: Decimal/Numeric column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_decimal_test (
    id integer,
    price numeric(10, 2),
    quantity numeric(5, 3)
) USING iceberg;

INSERT INTO iceberg_decimal_test VALUES
    (1, 99.99, 1.500),
    (2, 1234.56, 2.750),
    (3, 0.01, 0.001);

SELECT * FROM iceberg_decimal_test ORDER BY id;

-- Verify decimal arithmetic
SELECT id, price * quantity AS total FROM iceberg_decimal_test ORDER BY id;

DROP TABLE iceberg_decimal_test;

-- ============================================================================
-- Test 14: UUID column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_uuid_test (
    id integer,
    uuid_col uuid
) USING iceberg;

INSERT INTO iceberg_uuid_test VALUES
    (1, '550e8400-e29b-41d4-a716-446655440000'),
    (2, 'f47ac10b-58cc-4372-a567-0e02b2c3d479'),
    (3, '6ba7b810-9dad-11d1-80b4-00c04fd430c8');

SELECT * FROM iceberg_uuid_test ORDER BY id;

-- Verify UUID filtering
SELECT id FROM iceberg_uuid_test WHERE uuid_col = '550e8400-e29b-41d4-a716-446655440000';

DROP TABLE iceberg_uuid_test;

-- ============================================================================
-- Test 15: Binary (bytea) column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_binary_test (
    id integer,
    data bytea
) USING iceberg;

INSERT INTO iceberg_binary_test VALUES
    (1, '\x48454c4c4f'),
    (2, '\xDEADBEEF'),
    (3, '\x00010203');

SELECT id, encode(data, 'hex') AS data_hex FROM iceberg_binary_test ORDER BY id;

DROP TABLE iceberg_binary_test;

-- ============================================================================
-- Test 16: Integer Array column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_int_array_test (
    id integer,
    values integer[]
) USING iceberg;

INSERT INTO iceberg_int_array_test VALUES
    (1, '{1, 2, 3}'),
    (2, '{10, 20, 30, 40}'),
    (3, '{100}');

SELECT * FROM iceberg_int_array_test ORDER BY id;

-- Verify array element access
SELECT id, values[1] AS first_element FROM iceberg_int_array_test ORDER BY id;

DROP TABLE iceberg_int_array_test;

-- ============================================================================
-- Test 17: Text Array column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_text_array_test (
    id integer,
    tags text[]
) USING iceberg;

INSERT INTO iceberg_text_array_test VALUES
    (1, '{"apple", "banana", "cherry"}'),
    (2, '{"dog", "cat"}'),
    (3, '{"single"}');

SELECT * FROM iceberg_text_array_test ORDER BY id;

-- Verify array contains
SELECT id FROM iceberg_text_array_test WHERE 'apple' = ANY(tags);

DROP TABLE iceberg_text_array_test;

-- ============================================================================
-- Test 18: Bigint Array column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_bigint_array_test (
    id integer,
    big_values bigint[]
) USING iceberg;

INSERT INTO iceberg_bigint_array_test VALUES
    (1, '{1000000000000, 2000000000000}'),
    (2, '{9223372036854775807}'),
    (3, '{1, 2, 3, 4, 5}');

SELECT * FROM iceberg_bigint_array_test ORDER BY id;

DROP TABLE iceberg_bigint_array_test;

-- ============================================================================
-- Test 19: Float/Double Array column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_float_array_test (
    id integer,
    float_vals real[],
    double_vals double precision[]
) USING iceberg;

INSERT INTO iceberg_float_array_test VALUES
    (1, '{1.1, 2.2, 3.3}', '{1.111, 2.222}'),
    (2, '{3.14}', '{2.718281828, 3.141592653}');

SELECT * FROM iceberg_float_array_test ORDER BY id;

DROP TABLE iceberg_float_array_test;

-- ============================================================================
-- Test 20: Boolean Array column INSERT and SELECT
-- ============================================================================
CREATE TABLE iceberg_bool_array_test (
    id integer,
    flags boolean[]
) USING iceberg;

INSERT INTO iceberg_bool_array_test VALUES
    (1, '{true, false, true}'),
    (2, '{false, false}'),
    (3, '{true}');

SELECT * FROM iceberg_bool_array_test ORDER BY id;

DROP TABLE iceberg_bool_array_test;

