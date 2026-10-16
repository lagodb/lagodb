// Real Iceberg V3 nanosecond fields, which Spark SQL cannot declare.
// Runs in source-file mode with the fixture image's Iceberg 1.10.1 jars.
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public class NanosecondTimestamps {
  public static void main(String[] args) throws Exception {
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("rest", Map.of(
          "uri", args[0],
          "warehouse", args[1],
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
          "s3.endpoint", "http://minio:9000",
          "s3.path-style-access", "true"));
      Namespace namespace = Namespace.of(args[2]);
      if (!catalog.namespaceExists(namespace)) {
        catalog.createNamespace(namespace);
      }
      TableIdentifier name = TableIdentifier.of(namespace, args[3]);
      catalog.dropTable(name, true);
      Types.StructType nested = Types.StructType.of(
          Types.NestedField.required(5, "ts", Types.TimestampNanoType.withoutZone()),
          Types.NestedField.optional(6, "tz", Types.TimestampNanoType.withZone()),
          Types.NestedField.optional("label").withId(7).ofType(Types.StringType.get())
              .withInitialDefault(Expressions.lit("initial"))
              .withWriteDefault(Expressions.lit("written")).build());
      Schema schema = new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone()),
          Types.NestedField.optional(3, "tz", Types.TimestampNanoType.withZone()),
          Types.NestedField.optional(4, "detail", nested));
      Table table = catalog.buildTable(name, schema)
          .withPartitionSpec(PartitionSpec.unpartitioned())
          .withProperty("format-version", "3")
          .create();
      // Use the catalog-assigned schema (including its field IDs).
      schema = table.schema();
      nested = schema.findType("detail").asStructType();
      String location = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
      DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(location))
          .forTable(table)
          .createWriterFunc(GenericParquetWriter::create)
          .build();
      try (writer) {
        long[] nanos = {-1001, -1000, -999, -1, 0, 1, 999, 1000};
        for (int index = 0; index < nanos.length; index++) {
          OffsetDateTime time = Instant.ofEpochSecond(
              Math.floorDiv(nanos[index], 1_000_000_000L),
              Math.floorMod(nanos[index], 1_000_000_000L)).atOffset(ZoneOffset.UTC);
          GenericRecord detail = GenericRecord.create(nested);
          detail.setField("ts", time.toLocalDateTime());
          detail.setField("tz", time);
          detail.setField("label", "source");
          GenericRecord row = GenericRecord.create(schema);
          row.setField("id", index + 1);
          row.setField("ts", time.toLocalDateTime());
          row.setField("tz", time);
          row.setField("detail", detail);
          writer.write(row);
        }
        GenericRecord nullRow = GenericRecord.create(schema);
        nullRow.setField("id", 9);
        writer.write(nullRow);
      }
      table.newAppend().appendFile(writer.toDataFile()).commit();
    }
  }
}
