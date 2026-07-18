-- FULL routing: partition expansion, mixed native/provider, ANALYZE, and
-- database-wide expansion must preserve data and process each provider leaf.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA vacuum_full_routing_test;

CREATE TABLE vacuum_full_routing_test.partitioned_t (id integer)
PARTITION BY RANGE (id) USING iceberg;
CREATE TABLE vacuum_full_routing_test.partitioned_t_a
PARTITION OF vacuum_full_routing_test.partitioned_t
FOR VALUES FROM (0) TO (100) USING iceberg;
CREATE TABLE vacuum_full_routing_test.partitioned_t_b
PARTITION OF vacuum_full_routing_test.partitioned_t
FOR VALUES FROM (100) TO (200) USING iceberg;

INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (1);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (2);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (3);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (101);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (102);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (103);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (4);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (5);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (104);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (105);

CREATE TABLE vacuum_full_routing_test.heap_t (id integer);
INSERT INTO vacuum_full_routing_test.heap_t VALUES (10), (20);

VACUUM (FULL, ANALYZE)
    vacuum_full_routing_test.heap_t,
    vacuum_full_routing_test.partitioned_t;

SELECT array_agg(id ORDER BY id)
       = ARRAY[1, 2, 3, 4, 5, 101, 102, 103, 104, 105]
       AS partition_rows_preserved
FROM vacuum_full_routing_test.partitioned_t;
SELECT bool_and(current_data_objects = 1) AS each_leaf_compacted
FROM (VALUES
    ('vacuum_full_routing_test.partitioned_t_a'::regclass),
    ('vacuum_full_routing_test.partitioned_t_b'::regclass)
) AS leaves(relid)
CROSS JOIN LATERAL lakebase.table_maintenance_stats(leaves.relid);
SELECT array_agg(id ORDER BY id) = ARRAY[10, 20] AS heap_rows_preserved
FROM vacuum_full_routing_test.heap_t;

CREATE TABLE vacuum_full_routing_test.database_wide_t (id integer)
USING iceberg;
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (1);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (2);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (3);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (4);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (5);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (6);

VACUUM (FULL, SKIP_LOCKED);
SELECT current_data_objects = 1 AS database_wide_provider_routed
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.database_wide_t'
);
SELECT array_agg(id ORDER BY id) = ARRAY[1, 2, 3, 4, 5, 6]
       AS database_wide_rows_preserved
FROM vacuum_full_routing_test.database_wide_t;

CREATE TABLE vacuum_full_routing_test.security_t (id integer) USING iceberg;
INSERT INTO vacuum_full_routing_test.security_t VALUES (1);
INSERT INTO vacuum_full_routing_test.security_t VALUES (2);
INSERT INTO vacuum_full_routing_test.security_t VALUES (3);
INSERT INTO vacuum_full_routing_test.security_t VALUES (4);
INSERT INTO vacuum_full_routing_test.security_t VALUES (5);
INSERT INTO vacuum_full_routing_test.security_t VALUES (6);
CREATE ROLE vacuum_full_nonowner;
GRANT USAGE ON SCHEMA vacuum_full_routing_test TO vacuum_full_nonowner;
GRANT SELECT ON vacuum_full_routing_test.security_t TO vacuum_full_nonowner;
SET ROLE vacuum_full_nonowner;
SET client_min_messages = error;
VACUUM (FULL) vacuum_full_routing_test.security_t;
RESET client_min_messages;
RESET ROLE;
SELECT current_data_objects = 6 AS nonowner_did_not_rewrite
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.security_t'
);
VACUUM (FULL) vacuum_full_routing_test.security_t;
SELECT current_data_objects = 1 AS owner_rewrite_succeeded
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.security_t'
);
REVOKE SELECT ON vacuum_full_routing_test.security_t FROM vacuum_full_nonowner;
REVOKE USAGE ON SCHEMA vacuum_full_routing_test FROM vacuum_full_nonowner;
DROP ROLE vacuum_full_nonowner;

DROP SCHEMA vacuum_full_routing_test CASCADE;
DROP EXTENSION pg_iceberg_am CASCADE;
