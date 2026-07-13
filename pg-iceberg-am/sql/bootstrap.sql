CREATE SCHEMA iceberg;

CREATE TABLE iceberg.iceberg_metadata (
    relid regclass NOT NULL,
    metadata_location text,
    previous_metadata_location text,
    default_spec_id integer,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('iceberg.iceberg_metadata', '');
