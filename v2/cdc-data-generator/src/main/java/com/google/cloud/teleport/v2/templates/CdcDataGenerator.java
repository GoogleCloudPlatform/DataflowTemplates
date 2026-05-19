/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.dofn.BatchAndWriteFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.SinkConfigParser;
import com.google.cloud.teleport.v2.templates.transforms.GeneratePrimaryKey;
import com.google.cloud.teleport.v2.templates.transforms.GenerateTicks;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader;
import com.google.cloud.teleport.v2.templates.transforms.SelectTable;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ;
import com.google.common.io.CharStreams;
import com.jasonclawson.jackson.dataformat.hocon.HoconFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

@Template(
    name = "Cdc_Data_Generator",
    category = TemplateCategory.STREAMING,
    displayName = "CDC Data Generator",
    description = "A template to generate synthetic CDC data based on a source schema.",
    optionsClass = CdcDataGeneratorOptions.class,
    flexContainerName = "cdc-data-generator",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
public class CdcDataGenerator {

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    CdcDataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CdcDataGeneratorOptions.class);
    options.setStreaming(true);
    run(options);
  }

  public static PipelineResult run(CdcDataGeneratorOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    // Register SerializableCoder for Row class globally since we use dynamically
    // constructed schemas at runtime
    pipeline.getCoderRegistry().registerCoderForClass(Row.class, SerializableCoder.of(Row.class));

    SinkConfig sinkConfig;
    try {
      sinkConfig = SinkConfigParser.parse(options.getSinkType(), options.getSinkOptions());
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to parse sink configuration from " + options.getSinkOptions(), e);
    }

    SchemaConfig schemaConfig = null;
    if (options.getSchemaConfig() != null && !options.getSchemaConfig().isEmpty()) {
      try (ReadableByteChannel channel =
          FileSystems.open(FileSystems.matchNewResource(options.getSchemaConfig(), false))) {
        try (Reader reader =
            new InputStreamReader(Channels.newInputStream(channel), StandardCharsets.UTF_8)) {
          String content = CharStreams.toString(reader);
          ObjectMapper mapper = new ObjectMapper(new HoconFactory());
          schemaConfig = mapper.readValue(content, SchemaConfig.class);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read schema config from " + options.getSchemaConfig(), e);
      }
    }

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply(
            "LoadSchema",
            new SchemaLoader(
                options.getSinkType(),
                sinkConfig,
                options.getInsertQps(),
                options.getUpdateQps(),
                options.getDeleteQps(),
                schemaConfig));

    int keyParallelism = resolveKeyParallelism(options.getSinkType(), sinkConfig);

    // Generate ticks based on schema QPS
    PCollection<DataGeneratorTable> ticks =
        pipeline
            .apply(
                "TriggerTick", PeriodicImpulse.create().withInterval(Duration.standardSeconds(1)))
            .apply("GenerateTicks", new GenerateTicks(schemaView))
            .apply("RedistributeTicks", Redistribute.arbitrarily())
            .apply("SelectTable", new SelectTable(schemaView));

    // Generate Primary Keys
    PCollection<KV<String, Row>> pendingRows =
        ticks.apply(
            "GeneratePrimaryKey", new GeneratePrimaryKey(sinkConfig, options.getSinkType().name()));

    // Reshuffle based on Hash(TableName + PK) to ensure same PK goes to same worker
    PCollection<KV<Integer, GeneratedRecord>> reshuffledRows =
        pendingRows
            .apply(
                "MapToReshuffleKey",
                ParDo.of(
                    new DoFn<KV<String, Row>, KV<Integer, GeneratedRecord>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String tableName = c.element().getKey();
                        Row pkValues = c.element().getValue();
                        int hash = (tableName + pkValues.toString()).hashCode();
                        int shard = Math.abs(hash % keyParallelism);
                        c.output(KV.of(shard, GeneratedRecord.create(tableName, pkValues)));
                      }
                    }))
            .apply("Redistribute", Redistribute.byKey());

    Integer updateIntervalMs =
        (options.getUpdateInterval() != null && options.getUpdateInterval() >= 0
                ? options.getUpdateInterval()
                : 5)
            * 1000;
    Integer deleteIntervalMs =
        (options.getDeleteInterval() != null && options.getDeleteInterval() >= 0
                ? options.getDeleteInterval()
                : 5)
            * 1000;

    PCollection<String> dlqRecords =
        reshuffledRows
            .apply(
                "BatchAndWrite",
                ParDo.of(
                        new BatchAndWriteFn(
                            options.getSinkType(),
                            sinkConfig,
                            options.getBatchSize(),
                            options.getJdbcPoolSize(),
                            updateIntervalMs,
                            deleteIntervalMs,
                            schemaView))
                    .withSideInputs(schemaView))
            .setCoder(StringUtf8Coder.of());

    if (options.getDlqDirectory() != null && !options.getDlqDirectory().isEmpty()) {
      dlqRecords.apply(
          "WriteDLQRecordsToGCS",
          WriteDLQ.newBuilder()
              .withDlqDirectory(options.getDlqDirectory())
              .withTmpDirectory(options.getDlqDirectory() + "/tmp")
              .setIncludePaneInfo(true)
              .build());
    }

    return pipeline.run();
  }

  static int resolveKeyParallelism(
      CdcDataGeneratorOptions.SinkType sinkType, SinkConfig sinkConfig) {
    if (sinkType == CdcDataGeneratorOptions.SinkType.SPANNER) {
      return 50000;
    } else if (sinkType == CdcDataGeneratorOptions.SinkType.MYSQL) {
      if (!(sinkConfig instanceof MySqlSinkConfig)) {
        throw new IllegalArgumentException(
            "Sink configuration must be an instance of MySqlSinkConfig for MySQL sink type.");
      }
      MySqlSinkConfig mySqlConfig = (MySqlSinkConfig) sinkConfig;
      int shardCount = 1;
      if (mySqlConfig.getShards() != null && !mySqlConfig.getShards().isEmpty()) {
        shardCount = mySqlConfig.getShards().size();
      }
      return 5000 * shardCount;
    } else {
      throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
    }
  }
}
