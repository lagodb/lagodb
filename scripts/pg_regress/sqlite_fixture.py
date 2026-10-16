"""Empty SQL-catalog tables for PostgreSQL's local SQLite FDW regression.

Only bootstrap data lives here. Reads, writes, commit and rollback assertions
belong to iceberg_fdw_sqlite.sql. FixtureRuntime owns the directory and teardown.
"""

import json
import shutil
import sqlite3
import uuid
from contextlib import closing

from fixture_runtime import FixtureError, FixtureRuntime


class SqliteFixture:
    def __init__(self, runtime: FixtureRuntime) -> None:
        self.directory = runtime.directory / "sqlite"

    def provision(self) -> None:
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        try:
            with closing(sqlite3.connect(self.directory / "catalog.db")) as catalog:
                with catalog:
                    # Iceberg SQL catalog V1, also used by iceberg-lite's SQLite
                    # catalog. The catalog creates its namespace-property table.
                    catalog.execute("""
                        CREATE TABLE iceberg_tables (
                            catalog_name VARCHAR(255) NOT NULL,
                            table_namespace VARCHAR(255) NOT NULL,
                            table_name VARCHAR(255) NOT NULL,
                            metadata_location VARCHAR(1000),
                            previous_metadata_location VARCHAR(1000),
                            iceberg_type VARCHAR(5),
                            PRIMARY KEY (catalog_name, table_namespace, table_name)
                        )
                    """)
                    for name in ("items", "other_items"):
                        location = self.directory / name
                        metadata = location / "metadata" / f"00000-{uuid.uuid4()}.metadata.json"
                        metadata.parent.mkdir(parents=True)
                        metadata.write_text(json.dumps({
                            "format-version": 2,
                            "table-uuid": str(uuid.uuid4()),
                            "location": location.as_uri(),
                            "last-sequence-number": 0,
                            "last-updated-ms": 1700000000000,
                            "last-column-id": 2,
                            "current-schema-id": 0,
                            "schemas": [{
                                "type": "struct", "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "id", "required": False, "type": "int"},
                                    {"id": 2, "name": "payload", "required": False, "type": "string"},
                                ],
                            }],
                            "partition-specs": [{"spec-id": 0, "fields": []}],
                            "default-spec-id": 0,
                            "last-partition-id": 999,
                            "sort-orders": [{"order-id": 0, "fields": []}],
                            "default-sort-order-id": 0,
                            "current-snapshot-id": -1,
                            "snapshots": [],
                            "snapshot-log": [],
                            "metadata-log": [],
                        }) + "\n")
                        catalog.execute(
                            "INSERT INTO iceberg_tables VALUES (?, ?, ?, ?, NULL, 'TABLE')",
                            ("regress", "fdw_sqlite", name, metadata.as_uri()),
                        )
        except sqlite3.Error as error:
            raise FixtureError(f"SQLite fixture bootstrap failed: {error}") from error
