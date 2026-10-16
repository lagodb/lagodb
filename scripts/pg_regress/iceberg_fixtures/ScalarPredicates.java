// Iceberg primitives not all expressible in Spark SQL (UUID, fixed and time).
// One file mixes matching, nonmatching and NULL rows to expose reader filtering.
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public class ScalarPredicates {
  public static void main(String[] args) throws Exception {
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("rest", Map.of(
          "uri", args[0], "warehouse", args[1],
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
          "s3.endpoint", "http://minio:9000", "s3.path-style-access", "true"));
      Namespace namespace = Namespace.of(args[2]);
      if (!catalog.namespaceExists(namespace)) {
        catalog.createNamespace(namespace);
      }
      TableIdentifier name = TableIdentifier.of(namespace, args[3]);
      catalog.dropTable(name, true);
      Schema schema = new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "flag", Types.BooleanType.get()),
          Types.NestedField.optional(3, "payload", Types.BinaryType.get()),
          Types.NestedField.optional(4, "key", Types.UUIDType.get()),
          Types.NestedField.optional(5, "day", Types.DateType.get()),
          Types.NestedField.optional(6, "stamp", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(7, "instant", Types.TimestampType.withZone()),
          Types.NestedField.optional(8, "clock", Types.TimeType.get()),
          Types.NestedField.optional(9, "small", Types.IntegerType.get()),
          Types.NestedField.optional(10, "wide", Types.LongType.get()),
          Types.NestedField.optional(11, "fixed_payload", Types.FixedType.ofLength(2)),
          Types.NestedField.optional(12, "fixed16_payload", Types.FixedType.ofLength(16)));
      Table table = catalog.buildTable(name, schema)
          .withPartitionSpec(PartitionSpec.unpartitioned())
          .withProperty("format-version", "2").create();
      schema = table.schema();
      String location = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
      DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(location))
          .forTable(table).createWriterFunc(GenericParquetWriter::create).build();
      try (writer) {
        String[] keys = {"00000000-0000-0000-0000-000000000001",
            "80000000-0000-0000-0000-000000000000", null,
            "ffffffff-ffff-ffff-ffff-ffffffffffff"};
        byte[][] payloads = {{0, (byte) 255}, {(byte) 128, 0}, null, {(byte) 128, 0, 0}};
        int[] numbers = {-1, 2, 0, 4};
        for (int index = 0; index < 4; index++) {
          GenericRecord row = GenericRecord.create(schema);
          row.setField("id", index + 1);
          if (index != 2) {
            LocalDateTime stamp = LocalDateTime.of(2000, 1, 1, 0, 0)
                .plusNanos(index == 0 ? -1000 : index == 1 ? 0 : 1000);
            row.setField("flag", index != 1);
            row.setField("payload", ByteBuffer.wrap(payloads[index]));
            row.setField("key", UUID.fromString(keys[index]));
            row.setField("day", LocalDate.of(2000, 1, 1).plusDays(index == 0 ? -1 : index == 1 ? 0 : 1));
            row.setField("stamp", stamp);
            row.setField("instant", stamp.atOffset(ZoneOffset.UTC));
            row.setField("clock", LocalTime.of(index == 0 ? 0 : index == 1 ? 12 : 23, 0));
            row.setField("small", numbers[index]);
            row.setField("wide", (long) numbers[index]);
            row.setField("fixed_payload", new byte[] {payloads[index][0], payloads[index][1]});
            // Same width as UUID, but no UUID logical type or extension metadata.
            byte[] fixed16 = new byte[16];
            fixed16[0] = payloads[index][0];
            fixed16[1] = payloads[index][1];
            row.setField("fixed16_payload", fixed16);
          }
          writer.write(row);
        }
      }
      table.newAppend().appendFile(writer.toDataFile()).commit();
      // Every old row reads this newly added optional column as NULL.
      table.updateSchema().addColumn("later_flag", Types.BooleanType.get()).commit();
    }
  }
}
