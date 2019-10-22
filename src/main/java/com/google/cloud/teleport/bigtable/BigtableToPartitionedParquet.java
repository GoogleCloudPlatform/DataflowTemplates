/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.bigtable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to partitioned Parquet
 * files in GCS.
 */
public class BigtableToPartitionedParquet {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableToPartitionedParquet.class);

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {
    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description("The output location to write to (e.g. gs://mybucket/somefolder/)")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @Description("The prefix for each exported file in outputDirectory")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Parquet files in
   * GCS.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  public static String getTableSchema() {
    String avroSchemaStr = "";
    try {
      avroSchemaStr = new String(Files.readAllBytes(Paths
          .get("src/main/resources/schema/avro/bigtable.avsc")));
    } catch (IOException e) {
      LOG.error("failed to load schema");
    }
    return avroSchemaStr;
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    // Do not validate input fields if it is running as a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
      read = read.withoutValidation();
    }

    String schemaStr = getTableSchema();
    Schema schema = new Schema.Parser().parse(schemaStr);

    

    pipeline
        .apply("Read from Bigtable", read)
        .apply("Filtering & Transform to GenericRecord",ParDo.of(new BigtableToGenericRecord(schemaStr)))
        .setCoder(AvroCoder.of(schema))
        .apply("Windowing to 1 hour windows", Window.<GenericRecord>into(FixedWindows.of(Duration.standardHours(1))))
        .apply(
          "WriteToParquet",
          FileIO.<String, GenericRecord>writeDynamic()
                .by(record -> {
                  Long firstCellTS = (Long)((List<GenericRecord>)(record.get("cells"))).get(0).get("timestamp");
                  OffsetDateTime instant = Instant.ofEpochMilli(firstCellTS / 1000).atOffset(ZoneOffset.UTC);
                  return instant.getYear() + "/" + instant.getMonthValue() + "/" + instant.getDayOfMonth() + "/" + instant.getHour() + "/";
                })
                .withDestinationCoder(StringUtf8Coder.of())
                .via(ParquetIO.sink(schema))
                .to(options.getOutputDirectory())
                .withSharding(new PTransform<PCollection<GenericRecord>,PCollectionView<Integer>>("Window Sharding Transformation"){
                  @Override
                  public PCollectionView<Integer> expand(PCollection<GenericRecord> input) {
                    return input
                      .apply("Counting window size", Combine.globally(Count.<GenericRecord>combineFn()).withoutDefaults())
                      .apply("GenerateShardCount", ParDo.of(new CalculateShardsFn()))
                      .apply(View.asSingleton());
                  }
                })
                .withNaming((entityName) -> FileIO.Write.defaultNaming(entityName, ".parquet"))
        );


    return pipeline.run();
  }

  static class CalculateShardsFn extends DoFn<Long, Integer> {
    private final int maxShards = 100;
    @ProcessElement
    public void process(ProcessContext ctx) {
      ctx.output(calculateShards(ctx.element()));
    }
 
    private int calculateShards(long totalRecords) {
      if (totalRecords == 0) {
        // WriteFiles out at least one shard, even if there is no input.
        return 1;
      }
      
      //1 shard per 1000000 record
      int shards = (int) totalRecords/1000000;
      if (shards == 0) {
        return 1;
      }

      return Math.min(shards, maxShards);
    }
  }


  /** Translates Bigtable {@link Row} to Avro {@link BigtableRow}. */
  static class BigtableToGenericRecord extends DoFn<Row, GenericRecord> {
    private String schemaStr;
    private Schema schema = null;

    public BigtableToGenericRecord(String schemaStr) {
      this.schemaStr = schemaStr;
    }

    private Schema getSchema() {
      if (this.schema == null) {
        this.schema = new Schema.Parser().parse(schemaStr);
      }
      return this.schema;
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<GenericRecord> out) {
      Schema schema = this.getSchema();
      Schema cellsSchema = schema.getField("cells").schema().getElementType();

      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
      ByteBuffer key = ByteBuffer.wrap(BigtableToAvro.toByteArray(row.getKey()));
      recordBuilder.set("key", key);

      long timestamp = 0;
      List<GenericRecord> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(BigtableToAvro.toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            if (timestamp == 0) { //assign the timestamp tot he first cell of first column family
              timestamp = cell.getTimestampMicros();
            }
            ByteBuffer value = ByteBuffer.wrap(BigtableToAvro.toByteArray(cell.getValue()));
            GenericRecord genericCell = new GenericData.Record(cellsSchema);
            genericCell.put("family", familyName);
            genericCell.put("qualifier", qualifier);
            genericCell.put("timestamp", timestamp);
            genericCell.put("value", value);

            cells.add(genericCell);
          }
        }
      }
      if (timestamp == 0) {
        //no cell at all
        return;
      }

      recordBuilder.set("cells", cells);
      org.joda.time.Instant ins =  new org.joda.time.Instant(timestamp / 1000);
      out.outputWithTimestamp(recordBuilder.build(), ins); //set ts for each element to effectively window them later
    }
  }
}
