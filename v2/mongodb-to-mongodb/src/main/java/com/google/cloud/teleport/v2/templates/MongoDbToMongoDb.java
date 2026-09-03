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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadataCoder;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.MongoDbBackfillReader;
import com.google.cloud.teleport.v2.transforms.MongoDbBackfillReader.BackfillPartition;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeStreamReader.ChangeStreamPartition;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.google.cloud.teleport.v2.transforms.StatefulDeduplication;
import com.google.cloud.teleport.v2.transforms.UriSanitizer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dataflow template which copies data from one MongoDB database to another with CDC streaming support. */
@Template(
    name = "Mongodb_To_Mongodb",
    category = TemplateCategory.STREAMING,
    displayName = "MongoDB to MongoDB",
    description = "Copy or stream data from one MongoDB database to another.",
    flexContainerName = "mongodb-to-mongodb",
    optionsClass = MongoDbToMongoDb.Options.class)
public class MongoDbToMongoDb {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToMongoDb.class);

  public interface Options
      extends JavascriptTextTransformer.JavascriptTextTransformerOptions, StreamingOptions {
    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        description = "Source MongoDB Connection URI",
        helpText = "URI to connect to the source MongoDB cluster.")
    @Validation.Required
    String getSourceUri();

    void setSourceUri(String value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        description = "Target MongoDB Connection URI",
        helpText = "URI to connect to the target MongoDB cluster.")
    @Validation.Required
    String getTargetUri();

    void setTargetUri(String value);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        description = "Source MongoDB Database",
        helpText = "Database in the source MongoDB to read from.")
    @Validation.Required
    String getSourceDatabase();

    void setSourceDatabase(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Target MongoDB Database",
        helpText = "Database in the target MongoDB to write to.")
    @Validation.Required
    String getTargetDatabase();

    void setTargetDatabase(String value);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Source",
        optional = true,
        description = "Source MongoDB Collection",
        helpText =
            "Collection in the source MongoDB to read from. If not provided, all collections in the"
                + " database will be migrated.")
    String getSourceCollection();

    void setSourceCollection(String value);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Target",
        optional = true,
        description = "Target MongoDB Collection",
        helpText =
            "Collection in the target MongoDB to write to. If not provided, source collection names"
                + " will be used.")
    String getTargetCollection();

    void setTargetCollection(String value);

    @TemplateParameter.Integer(
        order = 7,
        groupName = "Source",
        optional = true,
        description = "Number of Backfill Read Splits",
        helpText =
            "Number of parallel coarse read splits generated per collection during historical"
                + " backfill (e.g. 1, 4, 8). Each split opens a sequential index-range cursor."
                + " Default is 1.")
    @Default.Integer(1)
    Integer getNumBackfillSplits();

    void setNumBackfillSplits(Integer value);

    @TemplateParameter.Integer(
        order = 8,
        groupName = "Target",
        optional = true,
        description = "Batch Size",
        helpText = "Number of documents in a bulk write.")
    @Default.Integer(5000)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @TemplateParameter.Integer(
        order = 9,
        groupName = "Target",
        optional = true,
        description = "Max Concurrent Async Writes",
        helpText = "Maximum number of concurrent asynchronous batch writes per worker.")
    @Default.Integer(10)
    Integer getMaxConcurrentAsyncWrites();

    void setMaxConcurrentAsyncWrites(Integer value);

    @TemplateParameter.Integer(
        order = 10,
        groupName = "Target",
        optional = true,
        description = "Max Write Retries",
        helpText = "Maximum number of retry attempts for transient failures during write.")
    @Default.Integer(3)
    Integer getMaxWriteRetries();

    void setMaxWriteRetries(Integer value);

    @TemplateParameter.Integer(
        order = 11,
        groupName = "Target",
        optional = true,
        description = "Initial Write Rate Per Worker",
        helpText =
            "Initial maximum documents/second written per worker thread during linear write rate"
                + " ramp-up. Set to <= 0 to disable throttling.")
    @Default.Integer(5000)
    Integer getInitialWriteRatePerWorker();

    void setInitialWriteRatePerWorker(Integer value);

    @TemplateParameter.Integer(
        order = 12,
        groupName = "Target",
        optional = true,
        description = "Write Rate Ramp Up Minutes",
        helpText =
            "Number of minutes between linear rate limit increases during write rate ramp-up.")
    @Default.Integer(5)
    Integer getWriteRateRampUpMinutes();

    void setWriteRateRampUpMinutes(Integer value);

    @TemplateParameter.Integer(
        order = 13,
        groupName = "Target",
        optional = true,
        description = "Max Write Rate Per Worker",
        helpText =
            "Maximum target documents/second per worker after completing ramp-up. Default is 25000.")
    @Default.Integer(25000)
    Integer getMaxWriteRatePerWorker();

    void setMaxWriteRatePerWorker(Integer value);

    @TemplateParameter.Integer(
        order = 14,
        groupName = "Target",
        optional = true,
        description = "Write Rate Ramp Up Steps",
        helpText = "Number of discrete linear step increases over the ramp-up period.")
    @Default.Integer(5)
    Integer getWriteRateRampUpSteps();

    void setWriteRateRampUpSteps(Integer value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "DLQ Directory",
        helpText =
            "Base path to store failed events. Events will be grouped by date and time, and"
                + " separated into 'retryable' and 'permanent' subdirectories.")
    String getDlqDirectory();

    void setDlqDirectory(String value);

    @TemplateParameter.Integer(
        order = 16,
        optional = true,
        description = "DLQ Max Retries",
        helpText = "Maximum number of times to retry events from DLQ.")
    @Default.Integer(3)
    Integer getDlqMaxRetries();

    void setDlqMaxRetries(Integer value);

    @TemplateParameter.Text(
        order = 17,
        groupName = "Source",
        optional = true,
        description = "Reconsume DLQ Path",
        helpText =
            "Path to read files from DLQ for reprocessing. If not provided, write DLQ path will be"
                + " used.")
    String getReconsumeDlqPath();

    void setReconsumeDlqPath(String value);

    @TemplateParameter.Boolean(
        order = 18,
        groupName = "Source",
        optional = true,
        description = "Read from DLQ",
        helpText = "If true, reads only from DLQ for retry. If false, reads from MongoDB.")
    @Default.Boolean(false)
    Boolean getReadFromDlq();

    void setReadFromDlq(Boolean value);

    @TemplateParameter.Enum(
        order = 19,
        groupName = "Source",
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("BACKFILL_AND_STREAMING"),
          @TemplateParameter.TemplateEnumOption("STREAMING_CDC"),
          @TemplateParameter.TemplateEnumOption("BATCH")
        },
        optional = true,
        description = "Migration Mode",
        helpText =
            "Migration mode: 'BACKFILL_AND_STREAMING' (historical backfill and streaming CDC in"
                + " parallel with stateful deduplication), 'STREAMING_CDC' (stream CDC only),"
                + " or 'BATCH' (historical backfill only).")
    @Default.String("BACKFILL_AND_STREAMING")
    String getMigrationMode();

    void setMigrationMode(String value);

    @TemplateParameter.Integer(
        order = 20,
        groupName = "Source",
        optional = true,
        description = "Number of Change Stream Splits",
        helpText =
            "Number of parallel change stream cursors per collection for high-throughput CDC"
                + " streaming (e.g. 1, 4, 8, 16).")
    @Default.Integer(1)
    Integer getNumChangeStreamSplits();

    void setNumChangeStreamSplits(Integer value);

    @TemplateParameter.Enum(
        order = 21,
        groupName = "Source",
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("updateLookup"),
          @TemplateParameter.TemplateEnumOption("whenAvailable"),
          @TemplateParameter.TemplateEnumOption("required"),
          @TemplateParameter.TemplateEnumOption("default")
        },
        optional = true,
        description = "Change Stream Full Document Strategy",
        helpText =
            "Strategy for fetching full document in change streams: 'updateLookup' (lookup full"
                + " doc from collection on updates, compatible with MongoDB 4+), 'whenAvailable'"
                + " (MongoDB 6+ post-image oplog extraction without extra lookup), 'required', or"
                + " 'default'.")
    @Default.String("updateLookup")
    String getChangeStreamFullDocument();

    void setChangeStreamFullDocument(String value);

    @TemplateParameter.Text(
        order = 22,
        groupName = "Source",
        optional = true,
        description = "Start At Operation Time",
        helpText =
            "Optional starting clusterTime (epoch seconds or ISO-8601 timestamp) to start change"
                + " stream cursors from. If omitted in BACKFILL_AND_STREAMING mode, captures the"
                + " current clusterTime before backfill starts.")
    String getStartAtOperationTime();

    void setStartAtOperationTime(String value);

    @TemplateParameter.Integer(
        order = 23,
        groupName = "Target",
        optional = true,
        description = "Number of Write Shards",
        helpText =
            "Number of parallel shards per collection for batched writes to prevent Windmill"
                + " single-key hotspots and maximize write parallelism. Default is 64.")
    @Default.Integer(64)
    Integer getNumWriteShards();

    void setNumWriteShards(Integer value);

    @TemplateParameter.Integer(
        order = 24,
        groupName = "Target",
        optional = true,
        description = "Max Buffering Duration Milliseconds",
        helpText =
            "Maximum duration in milliseconds to buffer documents before flushing a batch to target"
                + " MongoDB. Default is 200ms.")
    @Default.Integer(200)
    Integer getMaxBufferingDurationMs();

    void setMaxBufferingDurationMs(Integer value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static void run(Options options) {
    String migrationMode = options.getMigrationMode();
    if (migrationMode == null || migrationMode.isEmpty()) {
      migrationMode = "BACKFILL_AND_STREAMING";
    }

    boolean isStreamingMode =
        "BACKFILL_AND_STREAMING".equalsIgnoreCase(migrationMode)
            || "STREAMING_CDC".equalsIgnoreCase(migrationMode);

    if (isStreamingMode) {
      options.setStreaming(true);
      options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
    }

    Pipeline pipeline = Pipeline.create(options);

    String sourceUri = options.getSourceUri();
    String sourceDatabase = options.getSourceDatabase();
    String sourceCollection = options.getSourceCollection();
    String targetCollectionRaw = options.getTargetCollection();

    if ((sourceCollection == null || sourceCollection.isEmpty())
        && (targetCollectionRaw != null && !targetCollectionRaw.isEmpty())) {
      throw new IllegalArgumentException(
          "targetCollection cannot be specified when migrating an entire database without specifying sourceCollection.");
    }

    List<String> sourceCollections = new ArrayList<>();
    if (sourceCollection != null && !sourceCollection.isEmpty()) {
      sourceCollections.add(sourceCollection);
    } else {
      // List collections from source excluding internal system collections
      try (MongoClient mongoClient = MongoDbTransforms.createMongoClient(sourceUri)) {
        MongoDatabase db = mongoClient.getDatabase(sourceDatabase);
        for (String name : db.listCollectionNames()) {
          if (!name.startsWith("system.")) {
            sourceCollections.add(name);
          } else {
            LOG.info("Excluding internal MongoDB system collection '{}' from migration", name);
          }
        }
      }
    }

    String stagingLocation = options.as(DataflowPipelineOptions.class).getStagingLocation();
    String tmpDirectory = options.getTempLocation();
    if (tmpDirectory == null || tmpDirectory.isEmpty()) {
      if (stagingLocation != null && !stagingLocation.isEmpty()) {
        tmpDirectory =
            stagingLocation.endsWith("/") ? stagingLocation + "tmp" : stagingLocation + "/tmp";
      } else {
        tmpDirectory = "/tmp";
      }
    }

    String dlqDirectory = options.getDlqDirectory();
    if (dlqDirectory == null || dlqDirectory.isEmpty()) {
      dlqDirectory = tmpDirectory;
    }
    String baseDlqPath = dlqDirectory.endsWith("/") ? dlqDirectory : dlqDirectory + "/";
    String timestampPath = new SimpleDateFormat("yyyy-MM-dd/HH-mm-ss").format(new Date());
    String retryableDlqPath = baseDlqPath + timestampPath + "/retryable";
    String permanentDlqPath = baseDlqPath + timestampPath + "/permanent";

    LOG.info("Starting MongoDB-to-MongoDB Pipeline");
    LOG.info("  Source URI:              {}", UriSanitizer.sanitize(options.getSourceUri()));
    LOG.info("  Target URI:              {}", UriSanitizer.sanitize(options.getTargetUri()));
    LOG.info("  Source Database:         {}", options.getSourceDatabase());
    LOG.info("  Target Database:         {}", options.getTargetDatabase());
    LOG.info("  Source Collections:      {}", sourceCollections);
    LOG.info(
        "  Backfill Configuration:  coarse-split sequential cursor streaming (numBackfillSplits={})",
        options.getNumBackfillSplits() != null ? options.getNumBackfillSplits() : 1);
    LOG.info(
        "  Write Configuration:     batchSize={}, maxConcurrentAsyncWrites={}, maxWriteRetries={},"
            + " dlqMaxRetries={}",
        options.getBatchSize(),
        options.getMaxConcurrentAsyncWrites(),
        options.getMaxWriteRetries(),
        options.getDlqMaxRetries());
    LOG.info(
        "  Write Rate Limiting:     linear ramp-up from {} to {} docs/s/worker over {} mins in"
            + " {} steps",
        options.getInitialWriteRatePerWorker(),
        options.getMaxWriteRatePerWorker(),
        options.getWriteRateRampUpMinutes(),
        options.getWriteRateRampUpSteps());
    LOG.info("  Migration Mode:          {}", options.getMigrationMode());
    LOG.info("  Change Stream Splits:    {}", options.getNumChangeStreamSplits());
    LOG.info("  Change Stream Strategy:  {}", options.getChangeStreamFullDocument());
    LOG.info("  DLQ Base Directory:      {}", baseDlqPath + timestampPath);
    LOG.info("  DLQ Retryable Directory: {}", retryableDlqPath);
    LOG.info("  DLQ Permanent Directory: {}", permanentDlqPath);
    LOG.info(
        "  DLQ Inspection Command:  gcloud storage cat \"{}/**/output-*\" | head -n 5",
        permanentDlqPath);

    boolean includeBackfill =
        "BACKFILL_AND_STREAMING".equalsIgnoreCase(migrationMode)
            || "BATCH".equalsIgnoreCase(migrationMode);
    boolean includeCdc =
        "BACKFILL_AND_STREAMING".equalsIgnoreCase(migrationMode)
            || "STREAMING_CDC".equalsIgnoreCase(migrationMode);

    BsonTimestamp t0 = null;
    if (includeCdc) {
      if (options.getStartAtOperationTime() != null
          && !options.getStartAtOperationTime().isEmpty()) {
        try {
          long sec = Long.parseLong(options.getStartAtOperationTime());
          t0 = new BsonTimestamp((int) sec, 0);
        } catch (NumberFormatException nfe) {
          LOG.warn(
              "Could not parse startAtOperationTime '{}' as epoch seconds. Capturing current"
                  + " cluster time.",
              options.getStartAtOperationTime());
          t0 =
              MongoDbChangeStreamReader.captureCurrentClusterTime(
                  sourceUri, sourceDatabase);
        }
      } else {
        t0 =
            MongoDbChangeStreamReader.captureCurrentClusterTime(
                sourceUri, sourceDatabase);
      }
      LOG.info(
          "Captured initial cluster time T0: {} (seconds={}, inc={})",
          t0,
          t0.getTime(),
          t0.getInc());
    }

    if (options.getReadFromDlq() != null && options.getReadFromDlq()) {
      String reconsumePath = options.getReconsumeDlqPath();
      if (reconsumePath == null || reconsumePath.isEmpty()) {
        throw new IllegalArgumentException(
            "Reconsume DLQ path must be specified when reading from DLQ.");
      }
      PCollection<DocumentWithMetadata> documents = readFromDlq(pipeline, reconsumePath);
      PCollection<DocumentWithMetadata> validDocs =
          documents.apply(
              "ProcessDlq",
              new ProcessDocuments(options, retryableDlqPath, permanentDlqPath, tmpDirectory, true));
      validDocs.apply(
          "WriteDlq",
          new WriteDocuments(options, retryableDlqPath, permanentDlqPath, tmpDirectory));
    } else {
      List<PCollection<DocumentWithMetadata>> allStreams = new ArrayList<>();
      List<BackfillPartition> backfillPartitions = new ArrayList<>();
      List<ChangeStreamPartition> cdcPartitions = new ArrayList<>();

      MongoClient setupClient = null;
      try {
        setupClient = MongoDbTransforms.createMongoClient(options.getSourceUri());
      } catch (Exception e) {
        LOG.warn(
            "Could not connect to MongoDB during setup using shared client ({}). Using fallback.",
            e.getMessage());
      }

      try {
        for (String inputCollection : sourceCollections) {
          final String targetCollection =
              (targetCollectionRaw == null || targetCollectionRaw.isEmpty())
                  ? inputCollection
                  : targetCollectionRaw;

          if (includeBackfill) {
            int numBackfillSplits =
                options.getNumBackfillSplits() != null ? options.getNumBackfillSplits() : 1;
            try {
              backfillPartitions.addAll(
                  MongoDbBackfillReader.generatePartitions(
                      setupClient,
                      options.getSourceUri(),
                      options.getSourceDatabase(),
                      inputCollection,
                      targetCollection,
                      numBackfillSplits,
                      t0));
            } catch (Exception e) {
              LOG.warn(
                  "Could not generate partitioned splits for collection '{}' ({}). Falling back to"
                      + " single split.",
                  inputCollection,
                  e.getMessage());
              backfillPartitions.addAll(
                  MongoDbBackfillReader.generatePartitions(
                      null,
                      options.getSourceUri(),
                      options.getSourceDatabase(),
                      inputCollection,
                      targetCollection,
                      1,
                      t0));
            }
          }

          if (includeCdc && (sourceCollection != null && !sourceCollection.isEmpty())) {
            int numCdcSplits =
                options.getNumChangeStreamSplits() != null
                    ? options.getNumChangeStreamSplits()
                    : 1;
            try {
              cdcPartitions.addAll(
                  MongoDbChangeStreamReader.generatePartitions(
                      setupClient,
                      options.getSourceUri(),
                      options.getSourceDatabase(),
                      inputCollection,
                      targetCollection,
                      numCdcSplits,
                      t0,
                      options.getChangeStreamFullDocument()));
            } catch (Exception e) {
              LOG.warn(
                  "Could not generate change stream splits for collection '{}' ({}). Falling back"
                      + " to single split.",
                  inputCollection,
                  e.getMessage());
              cdcPartitions.addAll(
                  MongoDbChangeStreamReader.generatePartitions(
                      null,
                      options.getSourceUri(),
                      options.getSourceDatabase(),
                      inputCollection,
                      targetCollection,
                      1,
                      t0,
                      options.getChangeStreamFullDocument()));
            }
          }
        }

        if (includeCdc && (sourceCollection == null || sourceCollection.isEmpty())) {
          int numCdcSplits =
              options.getNumChangeStreamSplits() != null ? options.getNumChangeStreamSplits() : 1;
          LOG.info(
              "Configuring database-level change stream on database '{}' with {} parallel splits",
              options.getSourceDatabase(),
              numCdcSplits);
          try {
            cdcPartitions.addAll(
                MongoDbChangeStreamReader.generateDatabasePartitions(
                    setupClient,
                    options.getSourceUri(),
                    options.getSourceDatabase(),
                    numCdcSplits,
                    t0,
                    options.getChangeStreamFullDocument()));
          } catch (Exception e) {
            LOG.warn(
                "Could not generate database-level change stream splits ({}). Falling back to single"
                    + " split.",
                e.getMessage());
            cdcPartitions.addAll(
                MongoDbChangeStreamReader.generateDatabasePartitions(
                    null,
                    options.getSourceUri(),
                    options.getSourceDatabase(),
                    1,
                    t0,
                    options.getChangeStreamFullDocument()));
          }
        }
      } finally {
        if (setupClient != null) {
          try {
            setupClient.close();
          } catch (Exception ignored) {
          }
        }
      }

      if (includeBackfill && !backfillPartitions.isEmpty()) {
        PCollection<DocumentWithMetadata> backfillDocs =
            pipeline
                .apply(
                    "ReadBackfill",
                    new MongoDbBackfillReader.ReadPartitions(backfillPartitions))
                .setCoder(DocumentWithMetadataCoder.of());
        allStreams.add(backfillDocs);
      }

      if (includeCdc && !cdcPartitions.isEmpty()) {
        PCollection<DocumentWithMetadata> cdcDocs =
            pipeline
                .apply(
                    "ReadCDC",
                    new MongoDbChangeStreamReader.ReadPartitions(cdcPartitions))
                .setCoder(DocumentWithMetadataCoder.of());
        allStreams.add(cdcDocs);
      }

      PCollection<DocumentWithMetadata> documents;
      if (allStreams.size() == 1) {
        documents = allStreams.get(0);
      } else if (allStreams.size() > 1) {
        documents =
            PCollectionList.of(allStreams)
                .apply("MergeStreams", Flatten.pCollections());
      } else {
        throw new IllegalStateException("No streams configured to read.");
      }

      PCollection<DocumentWithMetadata> validDocs =
          documents.apply(
              "ProcessDocuments",
              new ProcessDocuments(
                  options, retryableDlqPath, permanentDlqPath, tmpDirectory, isStreamingMode));

      validDocs.apply(
          "WriteDocuments",
          new WriteDocuments(options, retryableDlqPath, permanentDlqPath, tmpDirectory));
    }

    pipeline.run();
  }

  /**
   * PTransform that processes documents: stateful deduplication, metric counting,
   * UDF transformation, and validation, routing process failures to DLQ.
   */
  public static class ProcessDocuments
      extends PTransform<PCollection<DocumentWithMetadata>, PCollection<DocumentWithMetadata>> {
    private final transient Options options;
    private final String retryableDlqPath;
    private final String permanentDlqPath;
    private final String tmpDirectory;
    private final boolean isStreamingMode;

    public ProcessDocuments(
        Options options,
        String retryableDlqPath,
        String permanentDlqPath,
        String tmpDirectory,
        boolean isStreamingMode) {
      this.options = options;
      this.retryableDlqPath = retryableDlqPath;
      this.permanentDlqPath = permanentDlqPath;
      this.tmpDirectory = tmpDirectory;
      this.isStreamingMode = isStreamingMode;
    }

    public ProcessDocuments(
        Options options, String retryableDlqPath, String permanentDlqPath, String tmpDirectory) {
      this(options, retryableDlqPath, permanentDlqPath, tmpDirectory, false);
    }

    @Override
    public PCollection<DocumentWithMetadata> expand(PCollection<DocumentWithMetadata> input) {
      PCollection<DocumentWithMetadata> documents = input;

      // Stateful Deduplication Stage
      if (isStreamingMode) {
        documents = documents.apply("Deduplicate", StatefulDeduplication.of());
      }

      // Metrics Stage
      documents =
          documents.apply(
              "CountTotalProcessed",
              ParDo.of(
                  new DoFn<DocumentWithMetadata, DocumentWithMetadata>() {
                    private final Counter totalProcessedDocuments =
                        Metrics.counter(MongoDbToMongoDb.class, "totalProcessedDocuments");

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      totalProcessedDocuments.inc();
                      c.output(c.element());
                    }
                  }));

      // UDF Stage
      if (options.getJavascriptTextTransformGcsPath() != null
          && !options.getJavascriptTextTransformGcsPath().isEmpty()) {
        TupleTag<DocumentWithMetadata> udfSuccessTag = new TupleTag<DocumentWithMetadata>() {};
        TupleTag<DocumentWithMetadata> udfFailureTag = new TupleTag<DocumentWithMetadata>() {};

        PCollectionTuple udfProcessed =
            documents.apply(
                "ApplyUDF",
                ParDo.of(
                        new MongoDbTransforms.ApplyUdfFn(
                            options.getJavascriptTextTransformGcsPath(),
                            options.getJavascriptTextTransformFunctionName(),
                            options.getJavascriptTextTransformReloadIntervalMinutes(),
                            udfFailureTag))
                    .withOutputTags(udfSuccessTag, TupleTagList.of(udfFailureTag)));

        // Write UDF Failures to DLQ
        udfProcessed
            .get(udfFailureTag)
            .apply(
                "WriteToDlq_UDF",
                new MongoDbTransforms.WriteToDlq(retryableDlqPath, permanentDlqPath, tmpDirectory));

        documents =
            udfProcessed
                .get(udfSuccessTag)
                .setCoder(DocumentWithMetadataCoder.of());
      }

      // Validation Stage with DLQ
      TupleTag<DocumentWithMetadata> successTag = new TupleTag<DocumentWithMetadata>() {};
      TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};

      PCollectionTuple processed =
          documents.apply(
              "Validate",
              ParDo.of(new ValidateFn(failureTag))
                  .withOutputTags(successTag, TupleTagList.of(failureTag)));

      // Write Process Failures to DLQ
      processed
          .get(failureTag)
          .apply(
              "WriteToDlq_Validate",
              new MongoDbTransforms.WriteToDlq(retryableDlqPath, permanentDlqPath, tmpDirectory));

      return processed.get(successTag).setCoder(DocumentWithMetadataCoder.of());
    }
  }

  /**
   * PTransform that writes valid documents in bulk to target MongoDB, routing write failures to DLQ.
   */
  public static class WriteDocuments
      extends PTransform<PCollection<DocumentWithMetadata>, PDone> {
    private final transient Options options;
    private final String retryableDlqPath;
    private final String permanentDlqPath;
    private final String tmpDirectory;

    public WriteDocuments(
        Options options, String retryableDlqPath, String permanentDlqPath, String tmpDirectory) {
      this.options = options;
      this.retryableDlqPath = retryableDlqPath;
      this.permanentDlqPath = permanentDlqPath;
      this.tmpDirectory = tmpDirectory;
    }

    @Override
    public PDone expand(PCollection<DocumentWithMetadata> validDocs) {
      PCollection<DocumentWithMetadata> writeFailures =
          validDocs.apply(
              "WriteToTarget",
              MongoDbTransforms.writeWithDlq()
                  .withUri(options.getTargetUri())
                  .withDatabase(options.getTargetDatabase())
                  .withBatchSize(options.getBatchSize())
                  .withNumWriteShards(options.getNumWriteShards())
                  .withMaxBufferingDurationMs(options.getMaxBufferingDurationMs())
                  .withMaxConcurrentAsyncWrites(options.getMaxConcurrentAsyncWrites())
                  .withMaxWriteRetries(options.getMaxWriteRetries())
                  .withDlqMaxRetries(options.getDlqMaxRetries())
                  .withInitialWriteRatePerWorker(
                      options.getInitialWriteRatePerWorker() != null
                          ? options.getInitialWriteRatePerWorker()
                          : 100)
                  .withMaxWriteRatePerWorker(
                      options.getMaxWriteRatePerWorker() != null
                          ? options.getMaxWriteRatePerWorker()
                          : 500)
                  .withWriteRateRampUpMinutes(
                      options.getWriteRateRampUpMinutes() != null
                          ? options.getWriteRateRampUpMinutes()
                          : 5)
                  .withWriteRateRampUpSteps(
                      options.getWriteRateRampUpSteps() != null
                          ? options.getWriteRateRampUpSteps()
                          : 5));

      writeFailures.apply(
          "WriteToDlq_Write",
          new MongoDbTransforms.WriteToDlq(retryableDlqPath, permanentDlqPath, tmpDirectory));

      return PDone.in(validDocs.getPipeline());
    }
  }

  public static class ValidateFn extends DoFn<DocumentWithMetadata, DocumentWithMetadata> {
    private final Counter contextCreationFailures =
        Metrics.counter(MongoDbToMongoDb.class, "contextCreationFailures");
    private final Counter validationPassed =
        Metrics.counter(MongoDbToMongoDb.class, "validationPassed");
    private final Counter validationFailedNullPayload =
        Metrics.counter(MongoDbToMongoDb.class, "validationFailedNullPayload");
    private final TupleTag<DocumentWithMetadata> failureTag;

    public ValidateFn(TupleTag<DocumentWithMetadata> failureTag) {
      this.failureTag = failureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DocumentWithMetadata item = c.element();
      if (item == null
          || (item.getDocument() == null
              && (item.getOperationType() == null
                  || (!item.getOperationType().isDelete()
                      && item.getOperationType() != DocumentWithMetadata.OperationType.DROP
                      && item.getOperationType() != DocumentWithMetadata.OperationType.RENAME)))) {
        contextCreationFailures.inc();
        validationFailedNullPayload.inc();
        c.output(
            failureTag,
            item != null
                ? item.withFailure(
                    "Null document payload",
                    DocumentWithMetadata.ErrorType.PERMANENT,
                    DocumentWithMetadata.FailureStage.VALIDATE)
                : DocumentWithMetadata.of(
                    null,
                    null,
                    0,
                    "Null element",
                    DocumentWithMetadata.ErrorType.PERMANENT,
                    null,
                    null,
                    DocumentWithMetadata.FailureStage.VALIDATE));
      } else {
        validationPassed.inc();
        c.output(item);
      }
    }
  }

  public static class ParseDlqFn extends DoFn<String, DocumentWithMetadata> {
    private final Counter retriedDocuments =
        Metrics.counter(MongoDbToMongoDb.class, "retriedDocuments");
    private final Counter contextCreationFailures =
        Metrics.counter(MongoDbToMongoDb.class, "contextCreationFailures");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String line = c.element();
      try {
        c.output(DocumentWithMetadata.fromDlqJson(line));
        retriedDocuments.inc();
      } catch (Exception e) {
        LOG.error("Failed to parse DLQ event", e);
        contextCreationFailures.inc();
      }
    }
  }

  private static PCollection<DocumentWithMetadata> readFromDlq(
      Pipeline pipeline, String reconsumePath) {
    PCollection<String> dlqStrings =
        pipeline.apply("ReadDlq", TextIO.read().from(reconsumePath + "/**"));

    return dlqStrings
        .apply("ParseDlq", ParDo.of(new ParseDlqFn()))
        .setCoder(DocumentWithMetadataCoder.of());
  }
}
