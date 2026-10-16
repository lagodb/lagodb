-- Metadata-only V3 table. Spark VOID maps to Iceberg unknown, including
-- nested elements. A skipped type must not undergo PG identifier validation.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer NOT NULL,
    unsupported_column_name_that_exceeds_postgresql_identifier_length_limit void,
    nested array<void>
) USING iceberg TBLPROPERTIES ('format-version'='3');
