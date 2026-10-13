-- Read-only JDBC catalog v1 over the managed catalog's existing MVCC rows.
-- Names follow PostgreSQL catalog changes; object locations retain their
-- CREATE-time identity. Partitioned Iceberg roots also own metadata rows.
-- Preserve native name columns so JDBC string predicates can use PostgreSQL's
-- catalog name indexes through its name/text comparison operators.
CREATE VIEW iceberg.iceberg_tables WITH (security_barrier = true) AS
SELECT pg_catalog.current_database()::text AS catalog_name,
       n.nspname AS table_namespace,
       c.relname AS table_name,
       m.metadata_location,
       m.previous_metadata_location,
       'TABLE'::varchar(5) AS iceberg_type
FROM iceberg.iceberg_metadata AS m
JOIN pg_catalog.pg_class AS c ON c.oid = m.relid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE m.metadata_location IS NOT NULL
  AND iceberg.catalog_is_object_tablespace(c.reltablespace)
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
  AND pg_catalog.has_table_privilege(c.oid, 'SELECT');

-- Standard JDBC queries use unqualified names. Follow PostgreSQL's implicit
-- pg_catalog lookup, without requiring client-specific search_path settings.
ALTER VIEW iceberg.iceberg_tables SET SCHEMA pg_catalog;
REVOKE ALL ON pg_catalog.iceberg_tables FROM PUBLIC;

-- Namespaces exist exactly when they contain an exposed managed table. There
-- is no independent namespace state or single warehouse location to maintain.
CREATE VIEW iceberg.iceberg_namespace_properties AS
SELECT DISTINCT catalog_name,
       table_namespace AS namespace,
       'exists'::text AS property_key,
       'true'::text AS property_value
FROM pg_catalog.iceberg_tables;

ALTER VIEW iceberg.iceberg_namespace_properties SET SCHEMA pg_catalog;
REVOKE ALL ON pg_catalog.iceberg_namespace_properties FROM PUBLIC;

COMMENT ON VIEW pg_catalog.iceberg_tables IS
    'Read-only Iceberg JDBC catalog v1 for object-backed managed tables; metadata pointers follow PostgreSQL transaction visibility.';
COMMENT ON VIEW pg_catalog.iceberg_namespace_properties IS
    'Read-only namespaces derived from accessible object-backed managed Iceberg tables.';
