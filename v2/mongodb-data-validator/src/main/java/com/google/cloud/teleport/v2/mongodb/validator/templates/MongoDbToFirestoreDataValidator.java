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
package com.google.cloud.teleport.v2.mongodb.validator.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.mongodb.validator.comparison.BsonDocumentComparator;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.ComparePartitionSliceFn;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.RecheckMismatchesFn;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.SliceReaderFactory;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.SliceReaderFactory.LiveMongoSliceReaderFactory;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.SliceStats;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice;
import com.google.cloud.teleport.v2.mongodb.validator.options.MongoDbDataValidatorOptions;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.PlanResult;
import com.google.cloud.teleport.v2.mongodb.validator.transforms.ValidationReportTransform;
import com.google.cloud.teleport.v2.mongodb.validator.util.UriSanitizer;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch Dataflow Flex Template that validates collection, secondary index, count, and deep BSON
 * document parity between a source MongoDB database and a target Cloud Firestore (MongoDB
 * compatibility) database using zero-shuffle co-partitioned {@code _id} merge-joins and delayed
 * primary mismatch rechecks.
 */
@Template(
    name = "Mongodb_To_Firestore_Data_Validator",
    category = TemplateCategory.BATCH,
    displayName = "MongoDB to Cloud Firestore Data Validation",
    description =
        "Batch pipeline that validates collection, secondary index, document count, and deep BSON"
            + " field parity between MongoDB and Cloud Firestore (MongoDB compatibility), writing"
            + " discrepancy reports to Cloud Storage and optionally BigQuery.",
    optionsClass = MongoDbDataValidatorOptions.class,
    flexContainerName = "mongodb-data-validator",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "Network connectivity from Dataflow workers to both the source MongoDB cluster and target"
          + " Cloud Firestore Enterprise database.",
      "Read permissions on source MongoDB and roles/datastore.viewer (or MongoDB SCRAM read"
          + " credentials) on target Cloud Firestore.",
      "A writable Cloud Storage bucket for JSONL discrepancy and summary outputs (and an existing"
          + " BigQuery dataset if --bigQueryDataset is specified)."
    })
public class MongoDbToFirestoreDataValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToFirestoreDataValidator.class);

  private static final DateTimeFormatter RUN_ID_TS_FMT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC);

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, throwable) ->
            LOG.error("Uncaught exception in thread {}", thread.getName(), throwable));
    MongoDbDataValidatorOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(MongoDbDataValidatorOptions.class);
    run(options);
  }

  public static PipelineResult run(MongoDbDataValidatorOptions options) {
    Instant startInstant = Instant.now();
    String sourceDb = options.getSourceDatabase();
    String targetDb =
        (options.getTargetDatabase() == null || options.getTargetDatabase().isBlank())
            ? sourceDb
            : options.getTargetDatabase().trim();
    String runId = resolveRunId(options, startInstant);

    LOG.info("Initializing MongoDB-to-Firestore Data Validator");
    LOG.info("  Run ID:          {}", runId);
    LOG.info("  Source URI:      {}", UriSanitizer.sanitize(options.getSourceUri()));
    LOG.info("  Target URI:      {}", UriSanitizer.sanitize(options.getTargetUri()));
    LOG.info("  Source Database: {}", sourceDb);
    LOG.info("  Target Database: {}", targetDb);
    LOG.info("  GCS Output Path: {}", options.getGcsOutputPath());

    PlanResult plan =
        CollectionAndIndexPlanner.planLive(
            options.getSourceUri(),
            options.getTargetUri(),
            sourceDb,
            targetDb,
            options.getCollections(),
            options.getCollectionMappings(),
            Boolean.TRUE.equals(options.getValidateIndexes()),
            options.getTargetChunkSize(),
            options.getMaxSplitsPerCollection(),
            runId,
            startInstant);

    SliceReaderFactory readerFactory =
        new LiveMongoSliceReaderFactory(
            options.getSourceUri(),
            options.getTargetUri(),
            sourceDb,
            targetDb,
            options.getSourceReadPreference());

    Pipeline pipeline = Pipeline.create(options);
    buildPipelineGraph(
        pipeline, options, runId, sourceDb, targetDb, startInstant, plan, readerFactory);
    return pipeline.run();
  }

  @VisibleForTesting
  public static void buildPipelineGraph(
      Pipeline pipeline,
      MongoDbDataValidatorOptions options,
      String runId,
      String sourceDb,
      String targetDb,
      Instant startInstant,
      PlanResult plan,
      SliceReaderFactory readerFactory) {
    BsonDocumentComparator.Config comparatorConfig =
        new BsonDocumentComparator.Config(
            Boolean.TRUE.equals(options.getStrictTypeChecking()),
            Boolean.TRUE.equals(options.getIgnoreSubdocumentFieldOrder()),
            BsonDocumentComparator.Config.parseIgnoredFields(options.getIgnoredFields()),
            10);

    PCollection<SliceStats> sliceStats;
    PCollection<MismatchedDocument> sliceCandidateMismatches;

    if (plan.getSlices().isEmpty()) {
      sliceStats =
          pipeline.apply("EmptySliceStats", Create.empty(SerializableCoder.of(SliceStats.class)));
      sliceCandidateMismatches =
          pipeline.apply(
              "EmptySliceMismatches", Create.empty(SerializableCoder.of(MismatchedDocument.class)));
    } else {
      PCollectionTuple sliceOutputs =
          pipeline
              .apply(
                  "CreateValidationSlices",
                  Create.of(plan.getSlices())
                      .withCoder(SerializableCoder.of(ValidationSlice.class)))
              .apply("DistributeSlices", Reshuffle.viaRandomKey())
              .apply(
                  "CompareSlices",
                  ParDo.of(
                          new ComparePartitionSliceFn(
                              runId,
                              sourceDb,
                              targetDb,
                              options.getBatchSize(),
                              options.getMaxBatchBytes(),
                              comparatorConfig,
                              readerFactory))
                      .withOutputTags(
                          ComparePartitionSliceFn.SLICE_STATS_TAG,
                          TupleTagList.of(ComparePartitionSliceFn.CANDIDATE_MISMATCH_TAG)));

      sliceStats =
          sliceOutputs
              .get(ComparePartitionSliceFn.SLICE_STATS_TAG)
              .setCoder(SerializableCoder.of(SliceStats.class));
      sliceCandidateMismatches =
          sliceOutputs
              .get(ComparePartitionSliceFn.CANDIDATE_MISMATCH_TAG)
              .setCoder(SerializableCoder.of(MismatchedDocument.class));
    }

    PCollectionTuple recheckOutputs =
        sliceCandidateMismatches
            .apply("DistributeCandidateMismatches", Reshuffle.viaRandomKey())
            .apply(
                "RecheckCandidateMismatches",
                ParDo.of(
                        new RecheckMismatchesFn(
                            options.getMismatchRecheckRetries(),
                            options.getMismatchRecheckDelayMs(),
                            options.getBatchSize(),
                            options.getMaxBatchBytes(),
                            comparatorConfig,
                            readerFactory))
                    .withOutputTags(
                        RecheckMismatchesFn.CONFIRMED_MISMATCH_TAG,
                        TupleTagList.of(RecheckMismatchesFn.RESOLVED_MATCH_COUNT_TAG)));

    PCollection<MismatchedDocument> confirmedDocMismatches =
        recheckOutputs
            .get(RecheckMismatchesFn.CONFIRMED_MISMATCH_TAG)
            .setCoder(SerializableCoder.of(MismatchedDocument.class));

    PCollection<KV<String, Long>> resolvedCounts =
        recheckOutputs
            .get(RecheckMismatchesFn.RESOLVED_MATCH_COUNT_TAG)
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    PCollection<MismatchedDocument> allConfirmedMismatches;
    if (plan.getCatalogAndIndexMismatches().isEmpty()) {
      allConfirmedMismatches = confirmedDocMismatches;
    } else {
      PCollection<MismatchedDocument> catalogMismatches =
          pipeline.apply(
              "CreateCatalogAndIndexMismatches",
              Create.of(plan.getCatalogAndIndexMismatches())
                  .withCoder(SerializableCoder.of(MismatchedDocument.class)));
      allConfirmedMismatches =
          PCollectionList.of(catalogMismatches)
              .and(confirmedDocMismatches)
              .apply("MergeAllMismatches", Flatten.pCollections());
    }

    sliceStats.apply(
        "WriteValidationReports",
        new ValidationReportTransform(
            runId,
            sourceDb,
            targetDb,
            options.getGcsOutputPath(),
            options.getBigQueryDataset(),
            startInstant.toString(),
            plan.getAllCollectionPairs().isEmpty()
                ? Collections.emptyMap()
                : plan.getAllCollectionPairs(),
            resolvedCounts,
            allConfirmedMismatches));
  }

  static String resolveRunId(MongoDbDataValidatorOptions options, Instant startInstant) {
    if (options.getRunId() != null && !options.getRunId().isBlank()) {
      return options.getRunId().trim();
    }
    String jobName = options.getJobName();
    if (jobName == null || jobName.isBlank()) {
      jobName = "mongodb_dv";
    }
    return jobName + "_" + RUN_ID_TS_FMT.format(startInstant);
  }
}
