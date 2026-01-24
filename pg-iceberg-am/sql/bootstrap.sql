CREATE SCHEMA IF NOT EXISTS lakehouse;

-- We use a custom table to store table options instead of reliance solely on pg_class.reloptions.
-- This bypasses the strict validation whitelist of the pg_class machinery.
CREATE TABLE IF NOT EXISTS lakehouse.table_options (
    relid regclass NOT NULL,
    options text[],
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('lakehouse.table_options', '');

CREATE TABLE IF NOT EXISTS lakehouse.iceberg_metadata (
    relid regclass NOT NULL,
    metadata_location text,
    previous_metadata_location text,
    default_spec_id integer,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('lakehouse.iceberg_metadata', '');