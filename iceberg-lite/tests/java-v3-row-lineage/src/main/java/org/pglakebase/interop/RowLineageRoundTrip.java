package org.pglakebase.interop;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * Loads a pg-lakebase-produced v3 table with Apache Iceberg Java, verifies the
 * allocated lineage ranges, performs a Java metadata commit, and verifies that
 * the ranges and scan remain readable after the round trip.
 */
public final class RowLineageRoundTrip {
  private RowLineageRoundTrip() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("usage: RowLineageRoundTrip TABLE_ROOT EXPECTED_ROWS");
    }
    long expectedRows = Long.parseLong(args[1]);
    Table table = new HadoopTables(new Configuration()).load(args[0]);
    State before = inspect(table, expectedRows);

    table.rewriteManifests().rewriteIf(ignored -> true).commit();
    table.refresh();
    State after = inspect(table, expectedRows);
    if (before.rows() != after.rows() || !before.firstRowIds().equals(after.firstRowIds())) {
      throw new AssertionError("Java manifest rewrite changed visible lineage: " + before + " -> " + after);
    }
    if (after.nextRowId() < before.nextRowId()) {
      throw new AssertionError("Java manifest rewrite moved next-row-id backwards: " + before + " -> " + after);
    }
    System.out.println("JAVA_V3_ROUNDTRIP_OK " + after);
  }

  private static State inspect(Table table, long expectedRows) throws Exception {
    TableMetadata metadata = ((BaseTable) table).operations().current();
    if (metadata.formatVersion() != 3) {
      throw new AssertionError("expected format v3, got v" + metadata.formatVersion());
    }
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null || snapshot.firstRowId() == null || snapshot.addedRows() == null) {
      throw new AssertionError("current v3 snapshot has no allocated row range");
    }

    long rows = 0;
    Map<String, Long> firstRowIds = new HashMap<>();
    try (var tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        rows = Math.addExact(rows, task.file().recordCount());
        Long firstRowId = task.file().firstRowId();
        if (firstRowId == null) {
          throw new AssertionError("Java could not inherit first_row_id for " + task.file().location());
        }
        firstRowIds.put(task.file().location(), firstRowId);
      }
    }
    if (rows != expectedRows) {
      throw new AssertionError("expected " + expectedRows + " rows, Java planned " + rows);
    }
    return new State(metadata.nextRowId(), snapshot.firstRowId(), snapshot.addedRows(), rows, firstRowIds);
  }

  private record State(
      long nextRowId,
      long snapshotFirstRowId,
      long snapshotAddedRows,
      long rows,
      Map<String, Long> firstRowIds) {}
}
