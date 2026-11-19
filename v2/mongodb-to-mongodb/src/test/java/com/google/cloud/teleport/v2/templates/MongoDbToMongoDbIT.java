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

import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.DLQ_UDF_POISON_DOC;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.DLQ_VALID_DOC;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.HETEROGENEOUS_BSON_DOCS;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.OVERLAP_DELETED_UUID_ID;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAMING_DELETED_BINARY_ID;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAMING_DELETED_UUID_ID;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAMING_SURVIVING_BINARY_ID;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAMING_SURVIVING_UUID_ID;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_POST_T0_FILTERED_DOC;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_POST_T0_POISON_DOC;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_POST_T0_VALID_DOC_1;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_POST_T0_VALID_DOC_2;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_PRE_T0_DOC_1;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.STREAM_PRE_T0_DOC_2;
import static com.google.cloud.teleport.v2.templates.MongoDbTestHelper.TRANSFORM_UDF_SCRIPT;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.MongoDbTestHelper.TestEnvironment;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end correctness integration tests for {@link MongoDbToMongoDb} ({@code
 * Mongodb_To_Mongodb}).
 *
 * <p>By default, each test migrates from an isolated {@code mongo:7.0} single-node replica-set
 * source container to an ephemeral Cloud Firestore Enterprise database with MongoDB compatibility
 * enabled (managed via {@link MongoDbTestHelper}).
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(MongoDbToMongoDb.class)
@RunWith(JUnit4.class)
public final class MongoDbToMongoDbIT extends TemplateTestBase {

  private static final Duration PIPELINE_TIMEOUT = Duration.ofMinutes(20);

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private TestEnvironment env;

  @Before
  public void setUp() throws Exception {
    if (MongoDbTestHelper.isDirectRunner(pipelineLauncher)) {
      gcsClient = null;
      artifactClient = null;
    }
    env =
        MongoDbTestHelper.setUpEnvironment(
            testId,
            testName,
            specPath,
            PROJECT,
            REGION,
            credentials,
            pipelineLauncher,
            gcsClient,
            this::getGcsPath,
            tempFolder);
  }

  @After
  public void tearDown() {
    if (env != null) {
      env.tearDown();
    }
  }

  /**
   * Suite 1: Exercises default {@code BACKFILL} mode (omitting {@code migrationMode}),
   * multi-collection auto-discovery (omitting {@code sourceCollection}), fine-grained adaptive
   * split probing ({@code targetBackfillChunkSize=3}), and byte/type-exact BSON fidelity across
   * heterogeneous {@code _id} and payload types into Cloud Firestore Enterprise.
   */
  @Test
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  public void backfill_heterogeneousBsonTypesAndMultiCollection_preservesAllDocuments()
      throws Exception {
    String bsonTypesCol = "col_bson_types";
    String secondaryCol = "col_secondary";

    List<RawBsonDocument> secondaryDocs = MongoDbTestHelper.createSecondaryDocs(15);
    env.sourceDb()
        .getCollection(bsonTypesCol, RawBsonDocument.class)
        .insertMany(HETEROGENEOUS_BSON_DOCS);
    env.sourceDb().getCollection(secondaryCol, RawBsonDocument.class).insertMany(secondaryDocs);

    // Omit migrationMode (tests default BACKFILL) and sourceCollection (tests multi-collection
    // discovery). Use targetBackfillChunkSize=3 to force multi-split probing across BSON types.
    LaunchConfig.Builder options =
        env.baseLaunchConfig()
            .addParameter("targetBackfillChunkSize", "3")
            .addParameter("maxBackfillSplits", "32")
            .addParameter("writeBatchSize", "5");

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, PIPELINE_TIMEOUT));
    assertThatResult(result).isLaunchFinished();

    env.assertCollectionParity(bsonTypesCol, HETEROGENEOUS_BSON_DOCS.size());
    env.assertCollectionParity(secondaryCol, secondaryDocs.size());
    MongoDbTestHelper.assertSpecialFloatsPreserved(env.targetDb(), bsonTypesCol);
  }

  /**
   * Suite 2: Exercises {@code BACKFILL_AND_STREAMING} mode with {@code
   * changeStreamFullDocument=whenAvailable}, concurrent updates, deletes (including {@code UUID}
   * {@code _id}), delete-then-reinsert resurrections, and net-new inserts during backfill overlap.
   */
  @Test
  public void backfillAndStreaming_concurrentOverlapMutations_deduplicatesAndConverges()
      throws Exception {
    String collection = "col_overlap_dedup";
    MongoDatabase srcDb = env.sourceDb();
    MongoDbTestHelper.createCollectionWithPreAndPostImages(srcDb, collection);

    MongoCollection<Document> srcCol = srcDb.getCollection(collection);
    srcCol.insertMany(MongoDbTestHelper.createOverlapBaselineDocs(40));

    LaunchConfig.Builder options =
        env.baseLaunchConfig()
            .addParameter("sourceCollection", collection)
            .addParameter("migrationMode", "BACKFILL_AND_STREAMING")
            .addParameter("changeStreamFullDocument", "whenAvailable")
            .addParameter("targetBackfillChunkSize", "10")
            .addParameter("writeBatchSize", "10")
            .addParameter("maxBufferingDurationMs", "100");

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Inject concurrent updates, deletes, resurrections, and new inserts while pipeline runs.
    for (int i = 0; i < 12; i++) {
      srcCol.updateOne(
          Filters.eq("_id", "doc_" + i),
          Updates.combine(Updates.set("v", 2), Updates.set("stage", "updated_v2")));
      srcCol.updateOne(
          Filters.eq("_id", "doc_" + i),
          Updates.combine(Updates.set("v", 3), Updates.set("stage", "updated_v3")));
    }
    for (int i = 12; i < 20; i++) {
      srcCol.deleteOne(Filters.eq("_id", "doc_" + i));
    }
    srcCol.deleteOne(Filters.eq("_id", OVERLAP_DELETED_UUID_ID));

    for (int i = 20; i < 25; i++) {
      String id = "doc_" + i;
      srcCol.deleteOne(Filters.eq("_id", id));
      srcCol.insertOne(
          new Document("_id", id)
              .append("v", 99)
              .append("resurrected", true)
              .append("stage", "resurrected_after_delete"));
    }
    for (int i = 0; i < 15; i++) {
      srcCol.insertOne(
          new Document("_id", "doc_new_" + i).append("v", 10).append("stage", "stream_insert"));
    }
    srcCol.insertOne(new Document("_id", "sentinel_overlap_done").append("v", 100));

    // Expected active count: 41 baseline - 8 deleted (doc_12..19) - 1 uuid deleted + 15 new + 1
    // sentinel = 48.
    final int expectedFinalCount = 48;
    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info, PIPELINE_TIMEOUT),
                env.collectionParityCheck(collection, expectedFinalCount));
    assertThatResult(result).meetsConditions();

    env.assertCollectionParity(collection, expectedFinalCount);
  }

  /**
   * Suite 3: Exercises {@code STREAMING_CDC} with {@code numChangeStreamSplits=4}, rapid same-key
   * mutation bursts ({@code INSERT -> UPDATE -> REPLACE -> UPDATE}, {@code INSERT -> UPDATE ->
   * DELETE}, {@code INSERT -> DELETE -> INSERT} resurrection, {@code UUID} {@code _id} delete), a
   * >64-event burst, and post-idle wake-up.
   */
  @Test
  public void streamingCdc_rapidSameKeyMutationsAndMultiPartition_coalescesAndConverges()
      throws Exception {
    String collection = "col_streaming_coalesce";
    MongoDatabase srcDb = env.sourceDb();
    MongoDbTestHelper.createCollectionWithPreAndPostImages(srcDb, collection);

    BsonTimestamp t0 = env.captureSourceClusterTime();
    MongoCollection<Document> srcCol = srcDb.getCollection(collection);

    LaunchConfig.Builder options =
        env.baseLaunchConfig()
            .addParameter("sourceCollection", collection)
            .addParameter("migrationMode", "STREAMING_CDC")
            .addParameter("numChangeStreamSplits", "4")
            .addParameter("changeStreamFullDocument", "updateLookup")
            .addParameter("startAtOperationTime", String.valueOf(t0.getTime()))
            .addParameter("writeBatchSize", "25")
            .addParameter("maxBufferingDurationMs", "100");

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // 1. Rapid same-key mutation chain: INSERT (v=1) -> UPDATE (v=2) -> REPLACE (v=3) -> UPDATE
    // (v=4)
    srcCol.insertOne(new Document("_id", "chain_key").append("v", 1).append("step", "insert"));
    srcCol.updateOne(
        Filters.eq("_id", "chain_key"),
        Updates.combine(Updates.set("v", 2), Updates.set("step", "update1")));
    srcCol.replaceOne(
        Filters.eq("_id", "chain_key"),
        new Document("_id", "chain_key").append("v", 3).append("step", "replace"));
    srcCol.updateOne(
        Filters.eq("_id", "chain_key"),
        Updates.combine(Updates.set("v", 4), Updates.set("step", "update2")));

    // 2. Rapid INSERT -> UPDATE -> DELETE (must not leave a zombie document on target)
    srcCol.insertOne(new Document("_id", "ephemeral_key").append("v", 1));
    srcCol.updateOne(Filters.eq("_id", "ephemeral_key"), Updates.set("v", 2));
    srcCol.deleteOne(Filters.eq("_id", "ephemeral_key"));

    // 3. Rapid INSERT -> DELETE -> INSERT resurrection
    srcCol.insertOne(new Document("_id", "resurrect_key").append("v", 1));
    srcCol.deleteOne(Filters.eq("_id", "resurrect_key"));
    srcCol.insertOne(
        new Document("_id", "resurrect_key").append("v", 2).append("resurrected", true));

    // 4. UUID _id, generic Binary (byte[]) _id, and cross-width numeric _id coalescing
    srcCol.insertOne(new Document("_id", STREAMING_DELETED_UUID_ID).append("v", 1));
    srcCol.updateOne(Filters.eq("_id", STREAMING_DELETED_UUID_ID), Updates.set("v", 2));
    srcCol.deleteOne(Filters.eq("_id", STREAMING_DELETED_UUID_ID));
    srcCol.insertOne(new Document("_id", STREAMING_SURVIVING_UUID_ID).append("v", 10));

    srcCol.insertOne(new Document("_id", STREAMING_DELETED_BINARY_ID).append("v", 1));
    srcCol.updateOne(Filters.eq("_id", STREAMING_DELETED_BINARY_ID), Updates.set("v", 2));
    srcCol.deleteOne(Filters.eq("_id", STREAMING_DELETED_BINARY_ID));
    srcCol.insertOne(new Document("_id", STREAMING_SURVIVING_BINARY_ID).append("v", 1));
    srcCol.updateOne(Filters.eq("_id", STREAMING_SURVIVING_BINARY_ID), Updates.set("v", 2));

    srcCol.insertOne(new Document("_id", 42).append("v", 1));
    srcCol.updateOne(Filters.eq("_id", 42L), Updates.set("v", 2));

    // 5. Burst of 70 inserts + 20 updates
    srcCol.insertMany(MongoDbTestHelper.createBurstDocs(70));
    for (int i = 0; i < 20; i++) {
      srcCol.updateOne(Filters.eq("_id", "burst_" + i), Updates.set("v", 2));
    }

    // Wait for Wave 1 (75 active documents: chain_key, resurrect_key, survivingUuid,
    // survivingBinary, numeric 42, burst_0..69)
    Result wave1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info, PIPELINE_TIMEOUT), env.collectionParityCheck(collection, 75));
    assertThatResult(wave1Result).meetsConditions();

    // Pause briefly so prefetcher becomes idle, then emit Wave 2 to verify requestFreshPollIfIdle()
    Thread.sleep(1500L);
    for (int i = 0; i < 5; i++) {
      srcCol.insertOne(new Document("_id", "post_idle_" + i).append("v", 1));
    }

    Result wave2Result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info, PIPELINE_TIMEOUT), env.collectionParityCheck(collection, 80));
    assertThatResult(wave2Result).meetsConditions();

    env.assertCollectionParity(collection, 80);
  }

  /**
   * Suite 4: Exercises {@code startAtOperationTime} filtering, JavaScript UDF transformation &
   * filtering ({@code return null}), UDF exception handling (verifying lazy {@code
   * originalDocument} preservation in retryable DLQ), and permanent write-failure DLQ routing.
   */
  @Test
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  public void streamingCdcAndBackfill_udfStartAtOperationTimeAndDlq_routesValidAndFailedDocs()
      throws Exception {
    String udfPath = env.stageUdfFile("input/transform_udf.js", TRANSFORM_UDF_SCRIPT);
    String streamDlqDir = env.resolveDlqDirectory("dlq-stream");

    String streamCollection = "col_udf_stream";
    MongoDatabase srcDb = env.sourceDb();
    srcDb.createCollection(streamCollection);
    RawBsonDocument streamPermRejectDoc =
        env.prepareBackfillDlqTargetAndGetRejectDoc(streamCollection);
    MongoCollection<RawBsonDocument> srcStreamCol =
        srcDb.getCollection(streamCollection, RawBsonDocument.class);

    // 1. Insert pre-T0 documents that must be skipped by startAtOperationTime.
    srcStreamCol.insertMany(ImmutableList.of(STREAM_PRE_T0_DOC_1, STREAM_PRE_T0_DOC_2));

    // Advance cluster clock by 2 seconds so T0 is strictly after pre_t0_1/pre_t0_2.
    Thread.sleep(2000L);
    BsonTimestamp t0 = MongoDbTestHelper.advanceAndCaptureClusterTime(srcDb);

    // 2. Insert post-T0 documents: valid, filtered (return null), poison (throws Error), and
    // permanent target rejection.
    srcStreamCol.insertMany(
        ImmutableList.of(
            STREAM_POST_T0_VALID_DOC_1,
            STREAM_POST_T0_FILTERED_DOC,
            STREAM_POST_T0_POISON_DOC,
            streamPermRejectDoc,
            STREAM_POST_T0_VALID_DOC_2));

    LaunchConfig.Builder streamOptions =
        env.baseLaunchConfig(testName + "_stream")
            .addParameter("sourceCollection", streamCollection)
            .addParameter("migrationMode", "STREAMING_CDC")
            .addParameter("startAtOperationTime", String.valueOf(t0.getTime()))
            .addParameter("javascriptTextTransformGcsPath", udfPath)
            .addParameter("javascriptTextTransformFunctionName", "transform")
            .addParameter("dlqDirectory", streamDlqDir)
            .addParameter("writeBatchSize", "5")
            .addParameter("maxBufferingDurationMs", "100");

    LaunchInfo streamInfo = launchTemplate(streamOptions);
    assertThatPipeline(streamInfo).isRunning();

    MongoCollection<Document> tgtStreamCol = env.targetDb().getCollection(streamCollection);
    Result streamResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(streamInfo, PIPELINE_TIMEOUT),
                MongoDbTestHelper.udfStreamConvergedCheck(tgtStreamCol),
                env.streamingDlqConvergedCheck(
                    "dlq-stream", streamDlqDir, "post_t0_poison", "dlq_permanent_reject_doc"));
    assertThatResult(streamResult).meetsConditions();

    // Verify pre-T0 documents, filtered document, poison document, and permanent reject document
    // were excluded from target, while live STREAMING_CDC DLQ flushed both failure tiers.
    assertThat(tgtStreamCol.find(Filters.eq("_id", "pre_t0_1")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "pre_t0_2")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "post_t0_filtered")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "post_t0_poison")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "dlq_permanent_reject_doc")).first()).isNull();

    String streamRetryableDlq = env.readDlqContents("dlq-stream", streamDlqDir, "retryable");
    assertThat(streamRetryableDlq).contains("post_t0_poison");
    assertThat(streamRetryableDlq).contains("keep_me");
    assertThat(streamRetryableDlq).contains("RETRYABLE");

    String streamPermanentDlq = env.readDlqContents("dlq-stream", streamDlqDir, "permanent");
    assertThat(streamPermanentDlq).contains("dlq_permanent_reject_doc");
    assertThat(streamPermanentDlq).contains("PERMANENT");

    Document valid1 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_1")).first();
    Document valid2 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_2")).first();
    assertThat(valid1.getBoolean("udfProcessed")).isTrue();
    assertThat(valid1.getInteger("doubledScore")).isEqualTo(20);
    assertThat(valid2.getBoolean("udfProcessed")).isTrue();
    assertThat(valid2.getInteger("doubledScore")).isEqualTo(50);

    // 3. Verify bounded BACKFILL retryable (UDF failure) and permanent (target write rejection)
    // DLQ routing.
    verifyBackfillDlqRouting(udfPath);
  }

  private void verifyBackfillDlqRouting(String udfPath) throws IOException {
    String backfillDlqDir = env.resolveDlqDirectory("dlq-backfill");
    String dlqCollection = "col_udf_dlq_backfill";

    RawBsonDocument permanentRejectDoc = env.prepareBackfillDlqTargetAndGetRejectDoc(dlqCollection);
    env.sourceDb()
        .getCollection(dlqCollection, RawBsonDocument.class)
        .insertMany(ImmutableList.of(DLQ_VALID_DOC, DLQ_UDF_POISON_DOC, permanentRejectDoc));

    LaunchConfig.Builder backfillDlqOptions =
        env.baseLaunchConfig(testName + "_backfill_dlq")
            .addParameter("sourceCollection", dlqCollection)
            .addParameter("migrationMode", "BACKFILL")
            .addParameter("javascriptTextTransformGcsPath", udfPath)
            .addParameter("javascriptTextTransformFunctionName", "transform")
            .addParameter("dlqDirectory", backfillDlqDir);

    LaunchInfo backfillInfo = launchTemplate(backfillDlqOptions);
    Result backfillResult =
        pipelineOperator().waitUntilDone(createConfig(backfillInfo, PIPELINE_TIMEOUT));
    assertThatResult(backfillResult).isLaunchFinished();

    MongoCollection<Document> tgtDlqCol = env.targetDb().getCollection(dlqCollection);
    assertThat(tgtDlqCol.countDocuments()).isEqualTo(1L);
    Document dlqValid = tgtDlqCol.find(Filters.eq("_id", "dlq_valid_doc")).first();
    assertThat(dlqValid).isNotNull();
    assertThat(dlqValid.getInteger("doubledScore")).isEqualTo(14);

    String retryableDlqJson = env.readDlqContents("dlq-backfill", backfillDlqDir, "retryable");
    assertThat(retryableDlqJson).contains("dlq_udf_poison_doc");
    assertThat(retryableDlqJson).contains("preserve_original_in_retryable_dlq");
    assertThat(retryableDlqJson).contains("Simulated UDF poison document failure");
    assertThat(retryableDlqJson).contains("RETRYABLE");

    String permanentDlqJson = env.readDlqContents("dlq-backfill", backfillDlqDir, "permanent");
    assertThat(permanentDlqJson).contains("dlq_permanent_reject_doc");
    assertThat(permanentDlqJson).contains("PERMANENT");
  }

  /**
   * Suite 5: Exercises shared-rank {@code String}/{@code Symbol} split discovery across all 4
   * mixed-rank layouts, same-second 24-bit {@code ObjectId} counter wraparound split probing,
   * bounded rate-limiter capping ({@code maxWriteRatePerWorker < initialWriteRatePerWorker} with
   * {@code writeRateRampUpMinutes=0}), streaming-windowed {@code WriteToDlq} output ({@code
   * streaming=true}), and {@code Pattern} (BSON regex) {@code _id} batch coalescing.
   */
  @Test
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  public void
      backfillAndStreaming_sharedRankSplitsWraparoundBoundedRateRegexAndStreamingDlq_preservesAll()
          throws Exception {
    MongoDatabase srcDb = env.sourceDb();

    String wrapCol = "col_oid_wraparound";
    List<RawBsonDocument> wrapDocs = MongoDbTestHelper.createSameSecondWraparoundObjectIdDocs();
    srcDb.getCollection(wrapCol, RawBsonDocument.class).insertMany(wrapDocs);

    String canonicalRareCol = "col_canonical_bson_types";
    List<RawBsonDocument> canonicalRareDocs = MongoDbTestHelper.createCanonicalRareBsonIdDocs();
    Map<String, List<RawBsonDocument>> sharedRankCollections =
        MongoDbTestHelper.createSharedRankStringSymbolCollections();
    if (!env.isFirestoreTarget()) {
      srcDb.getCollection(canonicalRareCol, RawBsonDocument.class).insertMany(canonicalRareDocs);
      for (Map.Entry<String, List<RawBsonDocument>> entry : sharedRankCollections.entrySet()) {
        srcDb.getCollection(entry.getKey(), RawBsonDocument.class).insertMany(entry.getValue());
      }
    }

    LaunchConfig.Builder backfillOpts =
        env.baseLaunchConfig(testName + "_backfill")
            .addParameter("targetBackfillChunkSize", "1")
            .addParameter("maxBackfillSplits", "16")
            .addParameter("writeBatchSize", "4")
            .addParameter("initialWriteRatePerWorker", "5000")
            .addParameter("maxWriteRatePerWorker", "1000")
            .addParameter("writeRateRampUpMinutes", "0");

    LaunchInfo backfillInfo = launchTemplate(backfillOpts);
    Result backfillResult =
        pipelineOperator().waitUntilDone(createConfig(backfillInfo, PIPELINE_TIMEOUT));
    assertThatResult(backfillResult).isLaunchFinished();

    env.assertCollectionParity(wrapCol, wrapDocs.size());
    if (!env.isFirestoreTarget()) {
      env.assertCollectionParity(canonicalRareCol, canonicalRareDocs.size());
      for (Map.Entry<String, List<RawBsonDocument>> entry : sharedRankCollections.entrySet()) {
        env.assertCollectionParity(entry.getKey(), entry.getValue().size());
      }
    }

    // Exercise WriteToDlq under streaming=true (windowed writes + near-maxTimestamp clamping) for
    // both retryable UDF failures and permanent target write rejections.
    String udfPath = env.stageUdfFile("input/stream_dlq_udf.js", TRANSFORM_UDF_SCRIPT);
    String streamDlqDir = env.resolveDlqDirectory("dlq-streaming-engine-backfill");
    String dlqCol = "col_streaming_engine_dlq";
    RawBsonDocument permanentRejectDoc = env.prepareBackfillDlqTargetAndGetRejectDoc(dlqCol);
    srcDb
        .getCollection(dlqCol, RawBsonDocument.class)
        .insertMany(ImmutableList.of(DLQ_VALID_DOC, DLQ_UDF_POISON_DOC, permanentRejectDoc));

    LaunchConfig.Builder streamDlqOpts =
        env.baseLaunchConfig(testName + "_stream_dlq")
            .addParameter("sourceCollection", dlqCol)
            .addParameter("migrationMode", "BACKFILL")
            .addParameter("streaming", "true")
            .addParameter("javascriptTextTransformGcsPath", udfPath)
            .addParameter("javascriptTextTransformFunctionName", "transform")
            .addParameter("dlqDirectory", streamDlqDir);

    LaunchInfo streamDlqInfo = launchTemplate(streamDlqOpts);
    Result streamDlqResult =
        pipelineOperator().waitUntilDone(createConfig(streamDlqInfo, PIPELINE_TIMEOUT));
    assertThatResult(streamDlqResult).isLaunchFinished();

    MongoCollection<Document> tgtDlqCol = env.targetDb().getCollection(dlqCol);
    assertThat(tgtDlqCol.countDocuments()).isEqualTo(1L);
    assertThat(tgtDlqCol.find(Filters.eq("_id", "dlq_valid_doc")).first()).isNotNull();

    String retryableDlqJson =
        env.readDlqContents("dlq-streaming-engine-backfill", streamDlqDir, "retryable");
    assertThat(retryableDlqJson).contains("dlq_udf_poison_doc");
    assertThat(retryableDlqJson).contains("RETRYABLE");

    String permanentDlqJson =
        env.readDlqContents("dlq-streaming-engine-backfill", streamDlqDir, "permanent");
    assertThat(permanentDlqJson).contains("dlq_permanent_reject_doc");
    assertThat(permanentDlqJson).contains("PERMANENT");

    if (!env.isFirestoreTarget()) {
      String regexColName = "col_regex_stream";
      MongoDbTestHelper.createCollectionWithPreAndPostImages(srcDb, regexColName);
      BsonTimestamp t0 = env.captureSourceClusterTime();
      MongoCollection<Document> srcRegexCol = srcDb.getCollection(regexColName);

      LaunchConfig.Builder regexStreamOpts =
          env.baseLaunchConfig(testName + "_regex_stream")
              .addParameter("sourceCollection", regexColName)
              .addParameter("migrationMode", "STREAMING_CDC")
              .addParameter("startAtOperationTime", String.valueOf(t0.getTime()))
              .addParameter("writeBatchSize", "10")
              .addParameter("maxBufferingDurationMs", "300");

      LaunchInfo regexInfo = launchTemplate(regexStreamOpts);
      assertThatPipeline(regexInfo).isRunning();

      Document caseInsensitiveId =
          new Document("re", Pattern.compile("^abc", Pattern.CASE_INSENSITIVE));
      Document caseSensitiveId = new Document("re", Pattern.compile("^abc"));
      srcRegexCol.insertOne(new Document("_id", caseInsensitiveId).append("v", 1));
      srcRegexCol.updateOne(Filters.eq("_id", caseInsensitiveId), Updates.set("v", 2));
      srcRegexCol.updateOne(Filters.eq("_id", caseInsensitiveId), Updates.set("v", 3));
      srcRegexCol.insertOne(new Document("_id", caseSensitiveId).append("v", 10));

      Result regexResult =
          pipelineOperator()
              .waitForConditionAndCancel(
                  createConfig(regexInfo, PIPELINE_TIMEOUT),
                  env.collectionParityCheck(regexColName, 2));
      assertThatResult(regexResult).meetsConditions();
      env.assertCollectionParity(regexColName, 2);
    }
  }
}
