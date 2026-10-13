# External managed-table access

Object-storage managed tables are discoverable through a read-only projection of
the Iceberg JDBC Catalog v1 interface. PostgreSQL publishes the existing managed
metadata pointers; Spark, Trino, and other Iceberg clients load those pointers
and read object storage with their own Iceberg implementation. LagoDB does not
plan or execute their reads.

## Catalog interface

`pg_catalog.iceberg_tables` exposes:

| Column | Meaning |
| --- | --- |
| `catalog_name` | Current PostgreSQL database name |
| `table_namespace` | PostgreSQL schema name |
| `table_name` | PostgreSQL relation name |
| `metadata_location` | Current metadata pointer visible to the catalog query |
| `previous_metadata_location` | Previous pointer recorded by the same catalog row |
| `iceberg_type` | `TABLE` (JDBC Catalog v1) |

`pg_catalog.iceberg_namespace_properties` derives an `exists=true` entry for each
namespace containing an accessible table. It stores no independent namespace
properties. Empty PostgreSQL schemas are not published.

`table_namespace`, `table_name`, and the derived `namespace` retain PostgreSQL's
native `name` type so name predicates can use the existing catalog indexes.
The PostgreSQL JDBC driver exposes these columns as JDBC `VARCHAR` / Java
`String`. Their database type name and length metadata differ from the upstream
JDBC catalog's `VARCHAR(255)` columns; this is a PostgreSQL managed-table read
interface, not a general-purpose writable JDBC catalog.

Both views use PostgreSQL's implicit `pg_catalog` search path. Clients need no
custom search path. Local managed tables and foreign tables are excluded.
Iceberg partitioned roots are included. Empty managed tables already have an
initial metadata file and are discoverable before their first insert.

The catalog name must match the database name. Table and schema renames follow
PostgreSQL's catalogs without moving object files. A database rename changes the
catalog name and requires clients to update their configuration. Names retain
their original case; clients must quote identifiers where necessary. Standard
JDBC namespace serialization uses dots as level separators, so PostgreSQL schema
names containing dots do not have a portable single-level namespace mapping.

This interface does not support external table creation, registration, writes,
namespace mutations, or Iceberg views. JDBC Catalog schema v1 is independent of
the table's Iceberg format version; each client must support the table's actual
format and Iceberg types.

## Permissions

Grant an existing external-reader role access explicitly:

```sql
GRANT SELECT ON pg_catalog.iceberg_tables,
                pg_catalog.iceberg_namespace_properties TO lake_reader;
GRANT USAGE ON SCHEMA analytics TO lake_reader;
GRANT SELECT ON analytics.events TO lake_reader;
```

Discovery checks the connecting role's schema `USAGE` and table-level `SELECT`.
The views do not grant access to the internal metadata table. Do not grant the
external-reader role writes to internal catalogs or PostgreSQL tables.

Configure separate read-only object-storage credentials in the external engine.
Catalog permissions do not grant or revoke object-storage access. Direct object
reads do not execute PostgreSQL row-level security or column-level restrictions;
grant object access only to readers authorized for the complete table contents.

## Client configuration

For Spark's standard Iceberg `SparkCatalog`, use the database name as the catalog
name (here `analytics_db`):

```properties
spark.sql.catalog.analytics_db=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.analytics_db.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog
spark.sql.catalog.analytics_db.uri=jdbc:postgresql://host:5432/analytics_db
spark.sql.catalog.analytics_db.jdbc.user=lake_reader
spark.sql.catalog.analytics_db.jdbc.schema-version=V1
spark.sql.catalog.analytics_db.jdbc.init-catalog-tables=false
spark.sql.catalog.analytics_db.warehouse=s3://example-bucket
spark.sql.catalog.analytics_db.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Supply the JDBC password through the deployment's secret configuration and the
appropriate Iceberg runtime, PostgreSQL driver, and FileIO dependencies. Configure
object-store region, endpoint, and authentication separately as required.

For Trino's Iceberg connector:

```properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=analytics_db
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://host:5432/analytics_db
iceberg.jdbc-catalog.connection-user=lake_reader
iceberg.jdbc-catalog.schema-version=V1
iceberg.jdbc-catalog.default-warehouse-dir=s3://example-bucket
iceberg.security=read_only
fs.s3.enabled=true
```

The required warehouse setting is a client default, not the authoritative table
location. Each metadata file contains its own table and file locations. Configure
the client's storage backend to access those locations directly.

Azure URI interoperability is deferred: managed Azure locations currently use
`az://`, which the standard Java Iceberg Azure FileIO and Trino Azure reader do
not accept. This change does not translate those locations or claim Azure
interoperability. S3/GCS clients also need their corresponding FileIO and storage
configuration. Engine-version interoperability must be established separately.

## Publication and retention

The views read the existing `iceberg.iceberg_metadata` rows using normal
PostgreSQL statement visibility. A separate client connection cannot see another
transaction's uncommitted pointers. There is no second publication catalog,
transaction-overlay export, or external-reader registration. Once the client has
loaded `metadata_location`, metadata interpretation, snapshot selection,
partition pruning, and file reads belong to that client. Client caching follows
the client's own refresh policy.

Readers must accept the existing managed-table lifecycle:

- Snapshot retention protects snapshots and files referenced by retained
  snapshots. Active queries do not pin files against expiration.
- Metadata retention protects the configured metadata-log files. Keeping an old
  JSON file does not retain every historical snapshot or data file it mentions.
- Orphan cleanup has its own age threshold and reachability rules. Its threshold
  is not an extra retention period for files removed by snapshot expiration.
- `DROP TABLE` removes the catalog entry and transactionally queues deletion of
  the managed object tree. After commit, cleanup proceeds without an external
  reader grace period. Rollback does not publish that deletion request.

Choose retention settings that cover query duration and client cache usage.
`previous_metadata_location` is a catalog pointer, not a file-retention lease.
