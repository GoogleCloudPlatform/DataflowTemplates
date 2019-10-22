package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.bigtable.v2.Row;
import com.google.cloud.teleport.bigtable.BigtableToPartitionedParquet.BigtableToGenericRecord;
import com.google.common.collect.ImmutableList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BigtableToPartitionedParquetTest. */
@RunWith(JUnit4.class)
public final class BigtableToPartitionedParquetTest {
  @Test
  public void applyBigtableToGenericRecordFn() throws Exception {
    Row bigtableRow1 = createBigtableRow("row1");
    bigtableRow1 = upsertBigtableCell(bigtableRow1, "family1", "column1", 1L, "value1");
    Row bigtableRow2 = createBigtableRow("row2");
    bigtableRow2 = upsertBigtableCell(bigtableRow2, "family2", "column2", 1L, "value2");
    final List<Row> bigtableRows = ImmutableList.of(bigtableRow1, bigtableRow2);

    String schemaStr = BigtableToPartitionedParquet.getTableSchema();
    Schema schema = new Schema.Parser().parse(schemaStr);
    Schema cellsSchema = schema.getField("cells").schema().getElementType();

    GenericRecord cell1 = new GenericData.Record(cellsSchema);
    cell1.put("family", "family1");
    cell1.put("qualifier", ByteBuffer.wrap("column1".getBytes()));
    cell1.put("timestamp", 1L);
    cell1.put("value", ByteBuffer.wrap("value1".getBytes()));
    GenericRecord record1 = new GenericRecordBuilder(schema)
      .set("key", ByteBuffer.wrap("row1".getBytes()))
      .set("cells", ImmutableList.of(cell1))
      .build();

    GenericRecord cell2 = new GenericData.Record(cellsSchema);
    cell2.put("family", "family2");
    cell2.put("qualifier", ByteBuffer.wrap("column2".getBytes()));
    cell2.put("timestamp", 1L);
    cell2.put("value", ByteBuffer.wrap("value2".getBytes()));
    GenericRecord record2 = new GenericRecordBuilder(schema)
      .set("key", ByteBuffer.wrap("row2".getBytes()))
      .set("cells", ImmutableList.of(cell2))
      .build();

    final List<GenericRecord> expectedGenericRows = ImmutableList.of(record1, record2);

    TestPipeline pipeline = TestPipeline.create();
    PCollection<GenericRecord> outputGenericRows = pipeline
        .apply("Read from Bigtable", Create.of(bigtableRows))
        .apply("Filter & Transform to GenericRecord",ParDo.of(new BigtableToGenericRecord(schemaStr)))
        .setCoder(AvroCoder.of(schema));
    System.err.println("HERE:"+expectedGenericRows.toString());
    PAssert.that(outputGenericRows).containsInAnyOrder(expectedGenericRows);
  }
}
