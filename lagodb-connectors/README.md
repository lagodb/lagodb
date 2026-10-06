# LagoDB Connectors

`lagodb_connectors` lets PostgreSQL read and write local files and object storage
through foreign tables and `COPY` commands. It supports S3, S3-compatible
storage, Google Cloud Storage, and Azure Blob Storage, with `text`, `csv`,
`json`, `avro`, and `parquet` data.

## How it works

The extension registers one foreign data wrapper, `lagodb_connectors`. A foreign
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

## Local files

Local paths refer to the PostgreSQL server's filesystem. Native JSON (NDJSON),
Avro, and Parquet COPY use the same format readers and encoders as object storage:

```sql
COPY events TO '/tmp/events.parquet';
COPY imported_events FROM '/tmp/events.parquet';
COPY events TO '/tmp/events.avro' WITH (format 'avro', compression 'snappy');
COPY events TO '/tmp/events.json.gz' WITH (format 'json');
COPY events TO '/tmp/export-without-suffix' WITH (format 'parquet');
```

The connector infers native formats from their suffix when `format` is absent.
An explicit `format 'text'`, `format 'csv'`, or `format 'binary'` retains
PostgreSQL COPY semantics even when the filename has a native-format suffix.
Local native COPY requires `pg_read_server_files` for imports and
`pg_write_server_files` for exports, including the usual superuser privileges.
Outputs must use absolute paths; inputs also accept paths relative to PostgreSQL's
data directory. Local COPY does not need a foreign server or user mapping, and
does not accept the `server` option. Use filesystem paths rather than `file://` URIs.

For foreign tables, create an optionless server; no user mapping is needed:

```sql
CREATE SERVER local_files FOREIGN DATA WRAPPER lagodb_connectors;

CREATE FOREIGN TABLE local_events ()
SERVER local_files OPTIONS (path '/tmp/events.parquet');

CREATE FOREIGN TABLE local_event_directory (id bigint, payload text)
SERVER local_files OPTIONS (path '/tmp/event-directory/', format 'json');

INSERT INTO local_event_directory VALUES (1, 'example');
SELECT * FROM local_event_directory;
```

Foreign-table local paths support all connector formats. A local path ending in
`/` selects a directory; every other local path selects a single file, including
filenames without a suffix when `format` is supplied. Directory scans recurse
through ordinary subdirectories, select matching format suffixes, and retain
sorted membership for rescans. Directory symlinks are not traversed. An empty
column list uses the existing schema inference rules.

For text, CSV, and JSON collections, omitted `compression` selects the decoder
from each file's suffix, so plain, gzip, and Zstandard files can share a directory
or object prefix. An explicit `compression` option, including `none`, overrides
suffix inference for every input file. Schema inference uses the same rule.
For writes, omitted compression follows the destination path's suffix; directory
and prefix writes default to uncompressed output.

An explicit local input path can name a FIFO for sequential formats, including
JSON and Avro. Opening it waits for a producer, following PostgreSQL COPY's
blocking behavior. Parquet uses the file's reported length and random reads;
callers are responsible for supplying a suitable input. Directory scans collect
regular files only. For a FIFO foreign
table, declare columns explicitly to avoid consuming a stream during schema
inference; each scan or rescan opens the FIFO again and needs a fresh stream.

Like PostgreSQL `file_fdw`, setting a local foreign-table path requires
`pg_read_server_files`. Thereafter, table privileges and server `USAGE` can share
reads with other roles. Local inserts additionally require `pg_write_server_files`
for the effective user. Single-file foreign tables remain read-only; directory
foreign tables support inserts that create new files.

Single-file COPY TO truncates an existing file and can leave partial output after
an error, following PostgreSQL's file-output semantics. Directory outputs use
the existing rolling size policy and unique partitioned filenames. Each complete
file is published after encoding; transaction and savepoint aborts delete files
created by that operation. Files are visible before commit, as with object-store
prefix output; directory foreign tables do not provide MVCC file membership.

## Configure object storage

Install the runtime and connector extensions, then create a foreign server and
user mapping. This S3-compatible example is suitable for MinIO and similar
services:

```sql
CREATE EXTENSION lagodb_base;
CREATE EXTENSION lagodb_connectors;

CREATE SERVER lagodb_s3
FOREIGN DATA WRAPPER lagodb_connectors
OPTIONS (
    provider 's3_compatible',
    endpoint 'http://127.0.0.1:9000',
    allow_http 'true'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER lagodb_s3
OPTIONS (
    access_key_id 'minioadmin',
    secret_access_key 'minioadmin'
);
```

Use `provider 's3'` for AWS S3, `provider 'gcs'` for `gs://` URIs, or
`provider 'azure'` for `az://` URIs. Provider-specific credentials belong in
the user mapping. When another role uses the server, grant it `USAGE` and
create an appropriate user mapping for that role.

Give every server eligible for implicit selection an explicit object scope:

```sql
ALTER SERVER lagodb_s3
OPTIONS (ADD scope 's3://analytics-bucket/');
```

When COPY does not specify `server`, the connector enumerates accessible
`lagodb_connectors` servers and selects the longest matching scope. Equal-length
matches are rejected as ambiguous, and servers without `scope` participate
only in explicit selection.

After selecting the server, the connector verifies that it uses
`lagodb_connectors`, its provider matches the URI, and the URI is within its
optional `scope`. The current role must have `USAGE` on the server. A user
mapping for that role, or a `PUBLIC` user mapping, must also exist.

## COPY with object URIs

The connector handles a `COPY` when its file source or destination begins
with `s3://`, `gs://`, or `az://`. PostgreSQL continues to handle `COPY`
through `STDIN`, `STDOUT`, a local text/CSV/binary file, or `PROGRAM`. An object-URI `COPY`
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
FOREIGN DATA WRAPPER lagodb_connectors
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
SERVER lagodb_s3
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
SERVER lagodb_s3
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
