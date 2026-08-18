# LagoDB Connectors

`lagodb_connectors` lets PostgreSQL read and write object storage through
foreign tables and object-URI `COPY` commands. It supports S3, S3-compatible
storage, Google Cloud Storage, and Azure Blob Storage, with `text`, `csv`,
`json`, `avro`, and `parquet` data.

## How it works

The extension registers one foreign data wrapper, `lakebase_fdw`. A foreign
server describes the storage provider, endpoint, region, and optional access
scope, while a user mapping holds credentials. Foreign tables and object-URI
`COPY` commands resolve storage through foreign servers and user mappings, so
credentials do not need to appear in object URIs.

An object path is either an exact object or a prefix. A path with a recognized
file suffix, such as `.csv`, `.json`, `.avro`, or `.parquet`, is an exact
object. A directory-style path ending in `/` is a prefix and requires an
explicit `format` option. Foreign tables can read exact objects or all matching
objects below a prefix. Inserts require a prefix and create new objects;
exact-object foreign tables are read-only. `UPDATE` and `DELETE` are not
supported.

Object-URI `COPY TO` can write an exact object or a prefix for every supported
format. Object-URI `COPY FROM` accepts exact objects for every format and also
accepts Parquet prefixes.

## Configure object storage

Install the runtime and connector extensions, then create a foreign server and
user mapping. This S3-compatible example is suitable for MinIO and similar
services:

```sql
CREATE EXTENSION pg_lakebase_runtime;
CREATE EXTENSION lagodb_connectors;

CREATE SERVER pg_lakebase_s3
FOREIGN DATA WRAPPER lakebase_fdw
OPTIONS (
    provider 's3_compatible',
    endpoint 'http://127.0.0.1:9000',
    allow_http 'true'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER pg_lakebase_s3
OPTIONS (
    access_key_id 'minioadmin',
    secret_access_key 'minioadmin'
);
```

Use `provider 's3'` for AWS S3, `provider 'gcs'` for `gs://` URIs, or
`provider 'azure'` for `az://` URIs. Provider-specific credentials belong in
the user mapping. When another role uses the server, grant it `USAGE` and
create an appropriate user mapping for that role.

Configure the default server for each URI scheme that the database uses:

```sql
ALTER DATABASE appdb
SET lagodb_connectors.default_s3_server = 'pg_lakebase_s3';
```

The available settings are `lagodb_connectors.default_s3_server` for `s3://`,
`lagodb_connectors.default_gcs_server` for `gs://`, and
`lagodb_connectors.default_azure_server` for `az://`. They have no built-in
server names. A database setting applies to new sessions; normal PostgreSQL
setting precedence allows a role or the current session to override it:

```sql
ALTER ROLE analytics
SET lagodb_connectors.default_s3_server = 'analytics_store';

SET lagodb_connectors.default_s3_server = 'development_store';
```

If neither the applicable setting nor the COPY `server` option is present,
the operation fails instead of selecting an arbitrary matching server.

After selecting the server, the connector verifies that it uses
`lakebase_fdw`, its provider matches the URI, and the URI is within its
optional `scope`. The current role must have `USAGE` on the server. A user
mapping for that role, or a `PUBLIC` user mapping, must also exist.

## COPY with object URIs

The connector handles a `COPY` when its file source or destination begins
with `s3://`, `gs://`, or `az://`. PostgreSQL continues to handle `COPY`
through `STDIN`, `STDOUT`, a local file, or `PROGRAM`. An object-URI `COPY`
does not create or require a foreign table.

With the S3 default configured above, an exact object with a supported suffix
needs no `WITH` clause. The connector infers its format from the suffix:

```sql
CREATE TABLE events (
    id bigint,
    occurred_at timestamptz,
    payload text
);

COPY events
TO 's3://analytics/exports/events.parquet';
```

Import an exact object into a PostgreSQL table:

```sql
CREATE TABLE imported_events (LIKE events);

COPY imported_events
FROM 's3://analytics/exports/events.parquet';
```

The same applies to CSV:

```sql
COPY events
TO 's3://analytics/exports/events.csv';

COPY imported_events
FROM 's3://analytics/exports/events.csv';
```

Use `WITH` only when it adds information that cannot be inferred. A prefix
needs `format`, because it has no file suffix:

```sql
COPY (
    SELECT id, occurred_at, payload
    FROM events
    WHERE occurred_at >= DATE '2026-01-01'
)
TO 's3://analytics/exports/2026/'
WITH (format 'parquet');
```

Non-default PostgreSQL COPY options remain available. For example, write a
CSV header with:

```sql
COPY events
TO 's3://analytics/exports/events-with-header.csv'
WITH (header true);
```

`server` overrides the scheme-specific default for one object-URI `COPY`:

```sql
CREATE SERVER object_store
FOREIGN DATA WRAPPER lakebase_fdw
OPTIONS (
    provider 's3_compatible',
    endpoint 'http://127.0.0.1:9000',
    allow_http 'true'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER object_store
OPTIONS (
    access_key_id 'minioadmin',
    secret_access_key 'minioadmin'
);

COPY events
TO 's3://analytics/exports/events.parquet'
WITH (server 'object_store');
```

`server` applies only to object-URI `COPY`. A foreign table selects its server
with the `SERVER` clause instead.

## Foreign tables

Create a foreign table over an exact object or prefix. This prefix table reads
all Parquet objects below `events/` and accepts inserts:

```sql
CREATE FOREIGN TABLE external_events (
    id bigint,
    occurred_at timestamptz,
    payload text
)
SERVER pg_lakebase_s3
OPTIONS (
    path 's3://analytics/events/',
    format 'parquet'
);

SELECT id, occurred_at, payload
FROM external_events
WHERE id >= 1000;

INSERT INTO external_events
VALUES (1001, clock_timestamp(), 'created by PostgreSQL');
```

For supported input data, an empty column list asks the connector to infer the
schema while creating the foreign table:

```sql
CREATE FOREIGN TABLE inferred_events ()
SERVER pg_lakebase_s3
OPTIONS (
    path 's3://analytics/events/',
    format 'parquet'
);
```

Foreign tables also compose with `COPY`. A foreign scan can feed an object
export:

```sql
COPY (
    SELECT *
    FROM external_events
    ORDER BY id
)
TO 's3://analytics/snapshots/events.json';
```

An object import can target a writable prefix foreign table. Here the source is
CSV, while the foreign table stores the inserted rows as Parquet:

```sql
COPY external_events
FROM 's3://analytics/incoming/events.csv'
WITH (header true);
```

Standard client streaming works as well:

```sql
COPY (SELECT * FROM external_events)
TO STDOUT WITH (format 'csv', header true);
COPY external_events FROM STDIN WITH (format 'csv', header true);
```

## COPY and foreign-table dependencies

Object-URI `COPY` depends on a foreign server and user mapping even when no
foreign table appears in the statement. The server comes from
the COPY `server` option, or from the URI scheme's configured default when the
option is absent.

When a foreign table participates, its storage configuration is resolved
independently. For example, in `COPY external_events FROM 's3://...'`, the
source URI uses the COPY-selected server while `external_events` uses the
server in its `SERVER` clause. Similarly, a query over a foreign table can be
exported to an object URI whose COPY server is different. Both servers and
their applicable user mappings must remain valid; neither side implicitly
inherits the other side's server.
