package com.google.cloud.teleport.v2.transforms;

import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link ParquetConverters} class. */
public class ParquetConvertersTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static final String RESOURCES_DIR = "ParquetConvertersTest/";

  private static final String FAKE_DIR = "FakeDirectory/";

  private static final String PARQUET_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "test_parquet_file.parquet").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "test_schema.json").getPath();

  /** Test {@link ParquetConverters.ReadParquetFile} Builder class. */
  @Test
  public void testReadParquetFile() {
    Schema schema = AvroConverters.getAvroSchema(SCHEMA_FILE_PATH);

    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", "007");
    genericRecord.put("state", "CA");
    genericRecord.put("price", 26.23);

    PCollection<GenericRecord> pCollection =
        pipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(PARQUET_FILE_PATH)
                .setSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(pCollection).containsInAnyOrder(genericRecord);

    pipeline.run();
  }

  /**
   * Test whether {@link ParquetConverters.ReadParquetFile} throws an exception if no Avro schema is
   * provided.
   */
  @Test
  public void testReadWithoutSchema() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withSchema(schema) called with null input.");

    pipeline.apply(
        ParquetConverters.ReadParquetFile.newBuilder()
            .withInputFileSpec(PARQUET_FILE_PATH)
            .withSchema(null)
            .build());

    pipeline.run();
  }

  /**
   * Test whether {@link ParquetConverters.ReadParquetFile} throws an exception if no input Parquet
   * file is provided.
   */
  @Test
  public void testReadWithoutInputFile() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withInputFileSpec(inputFileSpec) called with null input.");

    pipeline.apply(
        ParquetConverters.ReadParquetFile.newBuilder()
            .withInputFileSpec(null)
            .withSchema(SCHEMA_FILE_PATH)
            .build());

    pipeline.run();
  }

  /**
   * Test whether {@link ParquetConverters.WriteParquetFile} throws an exception if no Avro schema
   * is provided.
   */
  @Test
  public void testWriteWithoutSchema() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withSchema(schema) called with null input.");

    pipeline
        .apply(
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(PARQUET_FILE_PATH)
                .withSchema(SCHEMA_FILE_PATH)
                .build())
        .apply(
            ParquetConverters.WriteParquetFile.newBuilder()
                .withOutputFile(FAKE_DIR)
                .withSchema(null)
                .build());

    pipeline.run();
  }

  /**
   * Test whether {@link ParquetConverters.WriteParquetFile} throws an exception if no output
   * location is provided.
   */
  @Test
  public void testWriteWithoutOutputLocation() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withOutputFile(outputFile) called with null input.");

    pipeline
        .apply(
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(PARQUET_FILE_PATH)
                .withSchema(SCHEMA_FILE_PATH)
                .build())
        .apply(
            ParquetConverters.WriteParquetFile.newBuilder()
                .withOutputFile(null)
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    pipeline.run();
  }
}
