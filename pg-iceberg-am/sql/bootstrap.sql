CREATE SCHEMA iceberg;

CREATE TABLE iceberg.iceberg_metadata (
    relid regclass NOT NULL,
    metadata_location text,
    previous_metadata_location text,
    default_spec_id integer,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('iceberg.iceberg_metadata', '');

CREATE TABLE iceberg.automatic_maintenance_state (
    relid oid PRIMARY KEY,
    consecutive_failures integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT '-infinity',
    last_attempt_at timestamptz,
    last_success_at timestamptz,
    last_outcome text NOT NULL DEFAULT 'never',
    last_error text
);
