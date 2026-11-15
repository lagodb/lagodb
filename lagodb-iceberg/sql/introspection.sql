-- Catalog-only classification of managed Iceberg tables; it opens no
-- metadata file. iceberg.partition_spec(regclass) describes the rules.
-- An owner-rights view, like pg_tables over pg_class: callers read this
-- projection without any access to the private iceberg_metadata catalog,
-- whose maintenance state and metadata locations stay hidden.
CREATE VIEW iceberg.managed_tables AS
SELECT m.relid,
       n.nspname AS schemaname,
       c.relname AS tablename,
       m.is_partitioned
FROM iceberg.iceberg_metadata AS m
JOIN pg_catalog.pg_class AS c ON c.oid = m.relid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace;

GRANT SELECT ON iceberg.managed_tables TO PUBLIC;

COMMENT ON VIEW iceberg.managed_tables IS
    'Managed Iceberg tables and whether their current default partition spec partitions data.';
COMMENT ON FUNCTION iceberg.partition_spec(regclass) IS
    'Fields of the transaction-visible default Iceberg partition spec in spec order; an all-void spec lists fields but is unpartitioned.';
