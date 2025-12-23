/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.transformer.SourceRowToMutationDoFn;
import com.google.cloud.teleport.v2.writer.DeadLetterQueue;
import com.google.cloud.teleport.v2.writer.SpannerWriter;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateTableTransform extends PTransform<PBegin, PCollection<Void>> {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateTableTransform.class);

  private transient SourceDbToSpannerOptions options;
  private SpannerConfig spannerConfig;
  private Ddl ddl;
  private ISchemaMapper schemaMapper;
  private ReaderImpl reader;
  private String shardId;
  private SQLDialect sqlDialect;

  private Map<String, String> srcTableToShardIdColumnMap;

  public MigrateTableTransform(
      SourceDbToSpannerOptions options,
      SpannerConfig spannerConfig,
      Ddl ddl,
      ISchemaMapper schemaMapper,
      ReaderImpl reader,
      String shardId,
      Map<String, String> srcTableToShardIdColumnMap) {
    this.options = options;
    this.spannerConfig = spannerConfig;
    this.ddl = ddl;
    this.schemaMapper = schemaMapper;
    this.reader = reader;
    this.shardId = StringUtils.isEmpty(shardId) ? "" : shardId;
    this.sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());
    this.srcTableToShardIdColumnMap = srcTableToShardIdColumnMap;
  }

  @Override
  public PCollection<Void> expand(PBegin input) {
    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple rowsAndTables = input.apply("Read_rows", readerTransform.readTransform());
    PCollection<SourceRow> sourceRows = rowsAndTables.get(readerTransform.sourceRowTag());

    if (options.getGcsOutputDirectory() != null && !options.getGcsOutputDirectory().isEmpty()) {
      String avroDirectory;
      if (shardId.isEmpty()) {
        avroDirectory = options.getGcsOutputDirectory();
      } else {
        avroDirectory =
            FileSystems.matchNewResource(options.getGcsOutputDirectory(), true)
                .resolve(shardId, StandardResolveOptions.RESOLVE_DIRECTORY)
                .toString();
      }
      writeToGCS(sourceRows, avroDirectory);
    }

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();

    // Transform source data to Spanner Compatible Data
    SourceRowToMutationDoFn transformDoFn =
        SourceRowToMutationDoFn.create(
            schemaMapper, customTransformation, options.getInsertOnlyModeForSpannerMutations());
    PCollectionTuple transformationResult =
        sourceRows.apply(
            "Transform",
            ParDo.of(transformDoFn)
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(
                        Arrays.asList(
                            SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR,
                            SourceDbToSpannerConstants.FILTERED_EVENT_TAG))));

    // Write to Spanner
    SpannerWriter writer =
        new SpannerWriter(spannerConfig, options.getBatchSizeForSpannerMutations());
    SpannerWriteResult spannerWriteResult =
        writer.writeToSpanner(
            transformationResult
                .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
                .setCoder(SerializableCoder.of(RowContext.class)));
    PCollection<MutationGroup> failedMutations = spannerWriteResult.getFailedMutations();

    String outputDirectory = options.getOutputDirectory();
    if (!outputDirectory.endsWith("/")) {
      outputDirectory += "/";
    }

    // Dump Failed rows to DLQ
    String dlqDirectory = outputDirectory + "dlq/severe/" + shardId;
    LOG.info("DLQ directory: {}", dlqDirectory);
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            dlqDirectory, ddl, srcTableToShardIdColumnMap, sqlDialect, this.schemaMapper);
    dlq.failedMutationsToDLQ(failedMutations);
    dlq.failedTransformsToDLQ(
        transformationResult
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
            .setCoder(SerializableCoder.of(RowContext.class)));

    /*
     * Write filtered records to GCS
     */
    String filterEventsDirectory = outputDirectory + "filteredEvents/" + shardId;
    LOG.info("Filtered events directory: {}", filterEventsDirectory);
    DeadLetterQueue filteredEventsQueue =
        DeadLetterQueue.create(
            filterEventsDirectory, ddl, srcTableToShardIdColumnMap, sqlDialect, this.schemaMapper);
    filteredEventsQueue.filteredEventsToDLQ(
        transformationResult
            .get(SourceDbToSpannerConstants.FILTERED_EVENT_TAG)
            .setCoder(SerializableCoder.of(RowContext.class)));
    return spannerWriteResult.getOutput();
  }

  public WriteFilesResult<AvroDestination> writeToGCS(
      PCollection<SourceRow> sourceRows, String gcsOutputDirectory) {
    return sourceRows.apply(
        "WriteAvroToGCS",
        FileIO.<AvroDestination, SourceRow>writeDynamic()
            .by(
                (record) ->
                    AvroDestination.of(
                        record.tableName(), record.getPayload().getSchema().toString()))
            .via(
                Contextful.fn(record -> record.getPayload()),
                Contextful.fn(destination -> AvroIO.sink(destination.jsonSchema)))
            .withDestinationCoder(AvroCoder.of(AvroDestination.class))
            .to(gcsOutputDirectory)
            .withNumShards(0)
            .withNaming((SerializableFunction<AvroDestination, FileNaming>) AvroFileNaming::new));
  }

  class AvroFileNaming implements FileIO.Write.FileNaming {

    private final FileIO.Write.FileNaming defaultNaming;
    private final AvroDestination avroDestination;

    public AvroFileNaming(AvroDestination avroDestination) {
      defaultNaming =
          FileIO.Write.defaultNaming(DigestUtils.md5Hex(avroDestination.jsonSchema), ".avro");
      this.avroDestination = avroDestination;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      String subDir = avroDestination.name;
      return subDir
          + "/"
          + defaultNaming.getFilename(window, pane, numShards, shardIndex, compression);
    }
  }
}
