# Apache Iceberg Java v3 round trip

This fixture is the independent interoperability half of the v3 VACUUM test.
Create a format-v3 table, insert rows, run `VACUUM`/`VACUUM FULL`, and pass its
managed table root and expected visible row count to:

```sh
mvn -q compile exec:java \
  -Dexec.args='/absolute/or/object-store/table-root EXPECTED_ROWS'
```

The Java process loads and plans every current data file, requires inherited
`first_row_id` values, checks snapshot/table ranges and row counts, rewrites all
current manifests through Apache Iceberg Java, refreshes, and verifies that
visible file lineage and rows are unchanged while `next-row-id` remains
monotonic. PostgreSQL must then query the same table and compare its ordered
digest with the pre-round-trip digest. The fixture deliberately uses the
upstream Java implementation rather than duplicating its allocation rules in
Rust.

Dependency download and execution are part of the explicitly authorized test
workflow, not the static implementation pass.
