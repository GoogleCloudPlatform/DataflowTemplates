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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.Database.DataAccessMode;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ValidationOptions;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.dataflow.DirectRunnerClient;
import org.apache.beam.it.gcp.firestore.FirestoreAdminResourceManager;
import org.apache.beam.it.mongodb.MongoDBResourceManager;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end correctness integration tests for {@link MongoDbToMongoDb} ({@code
 * Mongodb_To_Mongodb}).
 *
 * <p>By default, each test migrates from an isolated {@code mongo:7.0} single-node replica-set
 * source container ({@link MongoDBResourceManager}) to an ephemeral Cloud Firestore Enterprise
 * database with MongoDB compatibility enabled ({@link FirestoreAdminResourceManager}). Optional
 * {@code -DsourceMongoUri}, {@code -DtargetMongoUri}, or {@code -DuseTestcontainersTarget=true}
 * properties can be supplied to override endpoints or run against local Docker containers.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@TemplateIntegrationTest(MongoDbToMongoDb.class)
@RunWith(JUnit4.class)
public final class MongoDbToMongoDbIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToMongoDbIT.class);
  private static final String MONGO_IMAGE_TAG = "7.0";

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private MongoDBResourceManager sourceMongoManager;
  private MongoDBResourceManager targetMongoManager;
  private FirestoreAdminResourceManager firestoreAdminResourceManager;

  private String sourceUri;
  private String targetUri;
  private String sourceDatabase;
  private String targetDatabase;

  private MongoClient sourceClient;
  private MongoClient targetClient;

  @Before
  public void setUp() throws Exception {
    if (isDirectRunner()) {
      gcsClient = null;
      artifactClient = null;
    }
    String uniqueSuffix =
        testId.replaceAll("[^a-zA-Z0-9]", "").toLowerCase()
            + "_"
            + UUID.randomUUID().toString().substring(0, 6);

    setUpSource(uniqueSuffix);
    setUpTarget(uniqueSuffix);

    sourceClient = MongoDbTransforms.createMongoClient(sourceUri);
    targetClient = MongoDbTransforms.createMongoClient(targetUri);
  }

  private void setUpSource(String uniqueSuffix) {
    String customSourceUri =
        TestProperties.getProperty("sourceMongoUri", "", TestProperties.Type.PROPERTY);
    if (customSourceUri != null && !customSourceUri.isEmpty()) {
      sourceUri = customSourceUri;
      sourceDatabase = "it_src_" + uniqueSuffix;
      return;
    }
    sourceMongoManager =
        MongoDBResourceManager.builder(testName).setContainerImageTag(MONGO_IMAGE_TAG).build();
    sourceUri = sourceMongoManager.getUri();
    sourceDatabase = sourceMongoManager.getDatabaseName();
  }

  private void setUpTarget(String uniqueSuffix) throws IOException {
    String customTargetUri =
        TestProperties.getProperty("targetMongoUri", "", TestProperties.Type.PROPERTY);
    boolean useTestcontainersTarget =
        Boolean.parseBoolean(
            TestProperties.getProperty(
                "useTestcontainersTarget", "false", TestProperties.Type.PROPERTY));

    if (customTargetUri != null && !customTargetUri.isEmpty()) {
      targetUri = customTargetUri;
      targetDatabase = "it_tgt_" + uniqueSuffix;
      return;
    }
    if (useTestcontainersTarget) {
      targetMongoManager =
          MongoDBResourceManager.builder(testName + "_target")
              .setContainerImageTag(MONGO_IMAGE_TAG)
              .build();
      targetUri = targetMongoManager.getUri();
      targetDatabase = targetMongoManager.getDatabaseName();
      return;
    }

    targetDatabase = "dst-" + UUID.randomUUID();
    firestoreAdminResourceManager =
        FirestoreAdminResourceManager.builder(testName)
            .setProject(PROJECT)
            .setRegion(REGION)
            .setCredentials(credentials)
            .build();
    firestoreAdminResourceManager.createDatabase(
        targetDatabase,
        Database.newBuilder()
            .setName(targetDatabase)
            .setType(DatabaseType.FIRESTORE_NATIVE)
            .setDatabaseEdition(DatabaseEdition.ENTERPRISE)
            .setMongodbCompatibleDataAccessMode(DataAccessMode.DATA_ACCESS_MODE_ENABLED)
            .setLocationId(REGION)
            .build());
    try (FirestoreAdminClient adminClient =
        FirestoreAdminClient.create(
            FirestoreAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build())) {
      Database db = adminClient.getDatabase("projects/" + PROJECT + "/databases/" + targetDatabase);
      String encodedToken = URLEncoder.encode(getAccessToken(), StandardCharsets.UTF_8);
      targetUri =
          String.format(
              "mongodb://access_token:%s@%s.%s.firestore.goog:443/%s"
                  + "?loadBalanced=true&tls=true&retryWrites=false"
                  + "&authMechanism=PLAIN&authSource=$external",
              encodedToken, db.getUid(), db.getLocationId(), targetDatabase);
    }
  }

  @After
  public void tearDown() {
    dropDatabaseQuietly(sourceClient, sourceDatabase);
    if (firestoreAdminResourceManager == null) {
      dropDatabaseQuietly(targetClient, targetDatabase);
    }
    closeClientQuietly(sourceClient);
    closeClientQuietly(targetClient);
    ResourceManagerUtils.cleanResources(
        sourceMongoManager, targetMongoManager, firestoreAdminResourceManager);
  }

  private String getAccessToken() throws IOException {
    GoogleCredentials googleCredentials =
        ((GoogleCredentials) credentials)
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    googleCredentials.refreshIfExpired();
    return googleCredentials.getAccessToken().getTokenValue();
  }

  /**
   * Suite 1: Exercises default {@code BACKFILL} mode (omitting {@code migrationMode}),
   * multi-collection auto-discovery (omitting {@code sourceCollection}), fine-grained adaptive
   * split probing ({@code targetBackfillChunkSize=3}), and byte/type-exact BSON fidelity across
   * heterogeneous {@code _id} and payload types into Cloud Firestore Enterprise.
   */
  @Test
  public void backfill_heterogeneousBsonTypesAndMultiCollection_preservesAllDocuments()
      throws Exception {
    String bsonTypesCol = "col_bson_types";
    String secondaryCol = "col_secondary";

    MongoCollection<RawBsonDocument> srcBsonCol =
        sourceClient.getDatabase(sourceDatabase).getCollection(bsonTypesCol, RawBsonDocument.class);
    MongoCollection<RawBsonDocument> srcSecondaryCol =
        sourceClient.getDatabase(sourceDatabase).getCollection(secondaryCol, RawBsonDocument.class);

    List<RawBsonDocument> heterogeneousDocs = buildHeterogeneousBsonDocuments();
    srcBsonCol.insertMany(heterogeneousDocs);

    List<RawBsonDocument> secondaryDocs = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      secondaryDocs.add(
          new RawBsonDocument(
              new Document("_id", "sec_" + i)
                  .append("index", i)
                  .append("group", i % 3)
                  .append("label", "secondary_collection_doc_" + i),
              MongoClientSettings.getDefaultCodecRegistry().get(Document.class)));
    }
    srcSecondaryCol.insertMany(secondaryDocs);

    // Omit migrationMode (tests default BACKFILL) and sourceCollection (tests multi-collection
    // discovery). Use targetBackfillChunkSize=3 to force multi-split probing across BSON types.
    LaunchConfig.Builder options =
        withTempLocation(
            LaunchConfig.builder(testName, specPath)
                .addParameter("sourceUri", sourceUri)
                .addParameter("targetUri", targetUri)
                .addParameter("sourceDatabase", sourceDatabase)
                .addParameter("targetDatabase", targetDatabase)
                .addParameter("targetBackfillChunkSize", "3")
                .addParameter("maxBackfillSplits", "32")
                .addParameter("writeBatchSize", "5")
                .addParameter("initialWriteRatePerWorker", "0"));

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(5)));
    assertThatResult(result).isLaunchFinished();

    assertCollectionParity(bsonTypesCol, heterogeneousDocs.size());
    assertCollectionParity(secondaryCol, secondaryDocs.size());

    // Explicitly verify IEEE 754 -0.0, NaN, and Int64 > 2^53 precision on the target database.
    MongoCollection<RawBsonDocument> tgtBsonCol =
        targetClient.getDatabase(targetDatabase).getCollection(bsonTypesCol, RawBsonDocument.class);
    RawBsonDocument negZeroDoc =
        tgtBsonCol.find(new BsonDocument("_id", new BsonString("special_floats"))).first();
    assertThat(negZeroDoc).isNotNull();
    assertThat(Double.doubleToRawLongBits(negZeroDoc.getDouble("negZero").getValue()))
        .isEqualTo(Double.doubleToRawLongBits(-0.0d));
    assertThat(Double.isNaN(negZeroDoc.getDouble("nanVal").getValue())).isTrue();
    assertThat(negZeroDoc.getInt64("aboveFloat53").getValue()).isEqualTo((1L << 53) + 1L);
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
    MongoDatabase srcDb = sourceClient.getDatabase(sourceDatabase);
    srcDb.createCollection(collection);
    enablePreAndPostImages(srcDb, collection);

    MongoCollection<Document> srcCol = srcDb.getCollection(collection);
    List<Document> baselineDocs = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      baselineDocs.add(
          new Document("_id", "doc_" + i).append("v", 1).append("stage", "baseline_" + i));
    }
    BsonBinary uuidToDelete =
        new BsonBinary(
            UUID.fromString("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"), UuidRepresentation.STANDARD);
    baselineDocs.add(new Document("_id", uuidToDelete).append("v", 1).append("stage", "uuid_base"));
    srcCol.insertMany(baselineDocs);

    LaunchConfig.Builder options =
        withTempLocation(
            LaunchConfig.builder(testName, specPath)
                .addParameter("sourceUri", sourceUri)
                .addParameter("targetUri", targetUri)
                .addParameter("sourceDatabase", sourceDatabase)
                .addParameter("targetDatabase", targetDatabase)
                .addParameter("sourceCollection", collection)
                .addParameter("migrationMode", "BACKFILL_AND_STREAMING")
                .addParameter("changeStreamFullDocument", "whenAvailable")
                .addParameter("targetBackfillChunkSize", "10")
                .addParameter("writeBatchSize", "10")
                .addParameter("maxBufferingDurationMs", "100")
                .addParameter("initialWriteRatePerWorker", "0"));

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
    srcCol.deleteOne(Filters.eq("_id", uuidToDelete));

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
                createConfig(info, Duration.ofMinutes(5)),
                collectionParityCheck(collection, expectedFinalCount));
    assertThatResult(result).meetsConditions();

    assertCollectionParity(collection, expectedFinalCount);
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
    MongoDatabase srcDb = sourceClient.getDatabase(sourceDatabase);
    srcDb.createCollection(collection);
    enablePreAndPostImages(srcDb, collection);

    BsonTimestamp t0 = captureClusterTime(sourceClient, sourceDatabase);
    MongoCollection<Document> srcCol = srcDb.getCollection(collection);

    LaunchConfig.Builder options =
        withTempLocation(
            LaunchConfig.builder(testName, specPath)
                .addParameter("sourceUri", sourceUri)
                .addParameter("targetUri", targetUri)
                .addParameter("sourceDatabase", sourceDatabase)
                .addParameter("targetDatabase", targetDatabase)
                .addParameter("sourceCollection", collection)
                .addParameter("migrationMode", "STREAMING_CDC")
                .addParameter("numChangeStreamSplits", "4")
                .addParameter("changeStreamFullDocument", "updateLookup")
                .addParameter("startAtOperationTime", String.valueOf(t0.getTime()))
                .addParameter("writeBatchSize", "25")
                .addParameter("maxBufferingDurationMs", "100")
                .addParameter("initialWriteRatePerWorker", "0"));

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

    // 4. UUID _id insert -> update -> delete and surviving UUID _id
    BsonBinary deletedUuid =
        new BsonBinary(
            UUID.fromString("11111111-2222-4333-8444-555555555555"), UuidRepresentation.STANDARD);
    BsonBinary survivingUuid =
        new BsonBinary(
            UUID.fromString("66666666-7777-4888-9999-aaaaaaaaaaaa"), UuidRepresentation.STANDARD);
    srcCol.insertOne(new Document("_id", deletedUuid).append("v", 1));
    srcCol.updateOne(Filters.eq("_id", deletedUuid), Updates.set("v", 2));
    srcCol.deleteOne(Filters.eq("_id", deletedUuid));
    srcCol.insertOne(new Document("_id", survivingUuid).append("v", 10));

    // 5. Burst of 70 inserts + 20 updates
    List<Document> burstDocs = new ArrayList<>();
    for (int i = 0; i < 70; i++) {
      burstDocs.add(new Document("_id", "burst_" + i).append("v", 1).append("idx", i));
    }
    srcCol.insertMany(burstDocs);
    for (int i = 0; i < 20; i++) {
      srcCol.updateOne(Filters.eq("_id", "burst_" + i), Updates.set("v", 2));
    }

    // Wait for Wave 1 (73 active documents: chain_key, resurrect_key, survivingUuid, burst_0..69)
    Result wave1Result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info, Duration.ofMinutes(4)), collectionParityCheck(collection, 73));
    assertThatResult(wave1Result).meetsConditions();

    // Pause briefly so prefetcher becomes idle, then emit Wave 2 to verify requestFreshPollIfIdle()
    Thread.sleep(1500L);
    for (int i = 0; i < 5; i++) {
      srcCol.insertOne(new Document("_id", "post_idle_" + i).append("v", 1));
    }

    Result wave2Result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info, Duration.ofMinutes(3)), collectionParityCheck(collection, 78));
    assertThatResult(wave2Result).meetsConditions();

    assertCollectionParity(collection, 78);
  }

  /**
   * Suite 4: Exercises {@code startAtOperationTime} filtering, JavaScript UDF transformation &
   * filtering ({@code return null}), UDF exception handling (verifying lazy {@code
   * originalDocument} preservation in retryable DLQ), and permanent write-failure DLQ routing.
   */
  @Test
  public void streamingCdcAndBackfill_udfStartAtOperationTimeAndDlq_routesValidAndFailedDocs()
      throws Exception {
    String udfScript =
        "function transform(inJson) {\n"
            + "  var doc = JSON.parse(inJson);\n"
            + "  if (doc.filterOut === true) {\n"
            + "    return null;\n"
            + "  }\n"
            + "  if (doc.poison === true) {\n"
            + "    throw new Error('Simulated UDF poison document failure');\n"
            + "  }\n"
            + "  var rawScore = (doc.score && doc.score.$numberInt !== undefined)\n"
            + "      ? parseInt(doc.score.$numberInt, 10)\n"
            + "      : (typeof doc.score === 'number' ? doc.score : 0);\n"
            + "  doc.udfProcessed = true;\n"
            + "  doc.doubledScore = {'$numberInt': String(rawScore * 2)};\n"
            + "  return JSON.stringify(doc);\n"
            + "}\n";
    String udfPath = stageUdfFile("input/transform_udf.js", udfScript);
    String streamDlqDir = resolveDlqDirectory("dlq-stream");

    String streamCollection = "col_udf_stream";
    MongoDatabase srcDb = sourceClient.getDatabase(sourceDatabase);
    srcDb.createCollection(streamCollection);
    MongoCollection<Document> srcStreamCol = srcDb.getCollection(streamCollection);

    // 1. Insert pre-T0 documents that must be skipped by startAtOperationTime.
    srcStreamCol.insertOne(new Document("_id", "pre_t0_1").append("score", 5));
    srcStreamCol.insertOne(new Document("_id", "pre_t0_2").append("score", 6));

    // Advance cluster clock by 2 seconds so T0 is strictly after pre_t0_1/pre_t0_2.
    Thread.sleep(2000L);
    BsonTimestamp t0 = advanceAndCaptureClusterTime(srcDb);

    // 2. Insert post-T0 documents: valid, filtered (return null), and poison (throws Error).
    srcStreamCol.insertOne(new Document("_id", "post_t0_valid_1").append("score", 10));
    srcStreamCol.insertOne(new Document("_id", "post_t0_filtered").append("filterOut", true));
    srcStreamCol.insertOne(
        new Document("_id", "post_t0_poison").append("poison", true).append("secret", "keep_me"));
    srcStreamCol.insertOne(new Document("_id", "post_t0_valid_2").append("score", 25));

    LaunchConfig.Builder streamOptions =
        withTempLocation(
            LaunchConfig.builder(testName + "_stream", specPath)
                .addParameter("sourceUri", sourceUri)
                .addParameter("targetUri", targetUri)
                .addParameter("sourceDatabase", sourceDatabase)
                .addParameter("targetDatabase", targetDatabase)
                .addParameter("sourceCollection", streamCollection)
                .addParameter("migrationMode", "STREAMING_CDC")
                .addParameter("startAtOperationTime", String.valueOf(t0.getTime()))
                .addParameter("javascriptTextTransformGcsPath", udfPath)
                .addParameter("javascriptTextTransformFunctionName", "transform")
                .addParameter("dlqDirectory", streamDlqDir)
                .addParameter("writeBatchSize", "5")
                .addParameter("maxBufferingDurationMs", "100")
                .addParameter("initialWriteRatePerWorker", "0"));

    LaunchInfo streamInfo = launchTemplate(streamOptions);
    assertThatPipeline(streamInfo).isRunning();

    MongoCollection<Document> tgtStreamCol =
        targetClient.getDatabase(targetDatabase).getCollection(streamCollection);
    Result streamResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(streamInfo, Duration.ofMinutes(4)),
                new ConditionCheck() {
                  @Override
                  protected String getDescription() {
                    return "Target collection has both transformed post-T0 valid documents";
                  }

                  @Override
                  protected CheckResult check() {
                    long count = tgtStreamCol.countDocuments();
                    Document d1 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_1")).first();
                    Document d2 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_2")).first();
                    boolean ready = count == 2 && d1 != null && d2 != null;
                    return new CheckResult(ready, "count=" + count);
                  }
                });
    assertThatResult(streamResult).meetsConditions();

    // Verify pre-T0 documents, filtered document, and poison document were excluded from target.
    assertThat(tgtStreamCol.find(Filters.eq("_id", "pre_t0_1")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "pre_t0_2")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "post_t0_filtered")).first()).isNull();
    assertThat(tgtStreamCol.find(Filters.eq("_id", "post_t0_poison")).first()).isNull();

    Document valid1 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_1")).first();
    Document valid2 = tgtStreamCol.find(Filters.eq("_id", "post_t0_valid_2")).first();
    assertThat(valid1.getBoolean("udfProcessed")).isTrue();
    assertThat(valid1.getInteger("doubledScore")).isEqualTo(20);
    assertThat(valid2.getBoolean("udfProcessed")).isTrue();
    assertThat(valid2.getInteger("doubledScore")).isEqualTo(50);

    // 3. Verify bounded BACKFILL retryable (UDF failure) and permanent (target write rejection)
    // DLQ routing.
    verifyBackfillDlqRouting(srcDb, udfPath);
  }

  private void verifyBackfillDlqRouting(MongoDatabase srcDb, String udfPath) throws IOException {
    String backfillDlqDir = resolveDlqDirectory("dlq-backfill");
    String dlqCollection = "col_udf_dlq_backfill";
    MongoDatabase tgtDb = targetClient.getDatabase(targetDatabase);

    Document permanentRejectDoc =
        new Document("_id", "dlq_permanent_reject_doc")
            .append("score", -5)
            .append("payload", "preserve_in_permanent_dlq");
    if (firestoreAdminResourceManager != null) {
      // Cloud Firestore Enterprise rejects reserved __fs_*__ field names per statement with
      // INVALID_ARGUMENT (MongoDB error code 2), which WriteBatchesFn routes to permanent DLQ.
      permanentRejectDoc.append("__fs_invalid__", "reserved_field_rejected_by_firestore");
    } else {
      tgtDb.createCollection(
          dlqCollection,
          new CreateCollectionOptions()
              .validationOptions(
                  new ValidationOptions().validator(Filters.gte("doubledScore", 0))));
    }

    MongoCollection<Document> srcDlqCol = srcDb.getCollection(dlqCollection);
    srcDlqCol.insertOne(new Document("_id", "dlq_valid_doc").append("score", 7));
    srcDlqCol.insertOne(
        new Document("_id", "dlq_udf_poison_doc")
            .append("poison", true)
            .append("payload", "preserve_original_in_retryable_dlq"));
    srcDlqCol.insertOne(permanentRejectDoc);

    LaunchConfig.Builder backfillDlqOptions =
        withTempLocation(
            LaunchConfig.builder(testName + "_backfill_dlq", specPath)
                .addParameter("sourceUri", sourceUri)
                .addParameter("targetUri", targetUri)
                .addParameter("sourceDatabase", sourceDatabase)
                .addParameter("targetDatabase", targetDatabase)
                .addParameter("sourceCollection", dlqCollection)
                .addParameter("migrationMode", "BACKFILL")
                .addParameter("javascriptTextTransformGcsPath", udfPath)
                .addParameter("javascriptTextTransformFunctionName", "transform")
                .addParameter("dlqDirectory", backfillDlqDir)
                .addParameter("initialWriteRatePerWorker", "0"));

    LaunchInfo backfillInfo = launchTemplate(backfillDlqOptions);
    Result backfillResult =
        pipelineOperator().waitUntilDone(createConfig(backfillInfo, Duration.ofMinutes(4)));
    assertThatResult(backfillResult).isLaunchFinished();

    MongoCollection<Document> tgtDlqCol = tgtDb.getCollection(dlqCollection);
    assertThat(tgtDlqCol.countDocuments()).isEqualTo(1L);
    Document dlqValid = tgtDlqCol.find(Filters.eq("_id", "dlq_valid_doc")).first();
    assertThat(dlqValid).isNotNull();
    assertThat(dlqValid.getInteger("doubledScore")).isEqualTo(14);

    String retryableDlqJson = readDlqContents("dlq-backfill", backfillDlqDir, "retryable");
    assertThat(retryableDlqJson).contains("dlq_udf_poison_doc");
    assertThat(retryableDlqJson).contains("preserve_original_in_retryable_dlq");
    assertThat(retryableDlqJson).contains("Simulated UDF poison document failure");
    assertThat(retryableDlqJson).contains("RETRYABLE");

    String permanentDlqJson = readDlqContents("dlq-backfill", backfillDlqDir, "permanent");
    assertThat(permanentDlqJson).contains("dlq_permanent_reject_doc");
    assertThat(permanentDlqJson).contains("PERMANENT");
  }

  private boolean isDirectRunner() {
    return pipelineLauncher instanceof DirectRunnerClient;
  }

  private LaunchConfig.Builder withTempLocation(LaunchConfig.Builder builder) throws IOException {
    if (isDirectRunner()) {
      Path localTemp =
          tempFolder
              .newFolder("beam-temp-" + UUID.randomUUID().toString().substring(0, 6))
              .toPath();
      builder.addParameter("tempLocation", localTemp.toString());
    }
    return builder;
  }

  private String stageUdfFile(String relativePath, String scriptContent) throws IOException {
    if (isDirectRunner() || gcsClient == null) {
      Path localUdf =
          tempFolder
              .newFile("transform_" + UUID.randomUUID().toString().substring(0, 6) + ".js")
              .toPath();
      Files.writeString(localUdf, scriptContent, StandardCharsets.UTF_8);
      return localUdf.toString();
    }
    gcsClient.createArtifact(relativePath, scriptContent);
    return getGcsPath(relativePath);
  }

  private String resolveDlqDirectory(String name) throws IOException {
    if (isDirectRunner() || gcsClient == null) {
      return tempFolder.newFolder(name).toPath().toString();
    }
    return getGcsPath(name);
  }

  private String readDlqContents(String gcsPrefix, String resolvedDlqDir, String tier)
      throws IOException {
    if (isDirectRunner() || gcsClient == null) {
      List<Path> matchingFiles;
      try (Stream<Path> walk = Files.walk(Path.of(resolvedDlqDir))) {
        matchingFiles =
            walk.filter(Files::isRegularFile)
                .filter(
                    p ->
                        p.toString().contains("/" + tier + "/error")
                            && p.toString().endsWith(".json"))
                .collect(Collectors.toList());
      }
      StringBuilder content = new StringBuilder();
      for (Path file : matchingFiles) {
        content.append(Files.readString(file, StandardCharsets.UTF_8));
      }
      return content.toString();
    }
    List<Artifact> dlqArtifacts =
        gcsClient.listArtifacts(gcsPrefix, Pattern.compile(".*" + tier + "/error.*\\.json"));
    StringBuilder dlqContent = new StringBuilder();
    for (Artifact artifact : dlqArtifacts) {
      dlqContent.append(new String(artifact.contents(), StandardCharsets.UTF_8));
    }
    return dlqContent.toString();
  }

  private void enablePreAndPostImages(MongoDatabase db, String collectionName) {
    db.runCommand(
        new Document("collMod", collectionName)
            .append("changeStreamPreAndPostImages", new Document("enabled", true)));
  }

  private BsonTimestamp captureClusterTime(MongoClient client, String dbName) {
    Document hello = client.getDatabase(dbName).runCommand(new Document("hello", 1));
    BsonTimestamp opTime = hello.get("operationTime", BsonTimestamp.class);
    assertThat(opTime).isNotNull();
    return opTime;
  }

  private BsonTimestamp advanceAndCaptureClusterTime(MongoDatabase db) {
    // Perform a lightweight metadata op so the replica set oplog operationTime advances to the
    // current wall-clock second.
    String tickCol = "_clock_tick_" + UUID.randomUUID().toString().substring(0, 6);
    db.getCollection(tickCol).insertOne(new Document("_id", "tick"));
    db.getCollection(tickCol).drop();
    Document hello = db.runCommand(new Document("hello", 1));
    return hello.get("operationTime", BsonTimestamp.class);
  }

  private ConditionCheck collectionParityCheck(String collectionName, int expectedCount) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return String.format(
            "Target collection '%s' matches source with %d documents",
            collectionName, expectedCount);
      }

      @Override
      protected CheckResult check() {
        try {
          Map<String, RawBsonDocument> srcDocs =
              readRawCollection(sourceClient, sourceDatabase, collectionName);
          Map<String, RawBsonDocument> tgtDocs =
              readRawCollection(targetClient, targetDatabase, collectionName);
          if (srcDocs.size() != expectedCount || tgtDocs.size() != expectedCount) {
            return new CheckResult(
                false,
                String.format(
                    "srcCount=%d, tgtCount=%d, expected=%d",
                    srcDocs.size(), tgtDocs.size(), expectedCount));
          }
          for (Map.Entry<String, RawBsonDocument> entry : srcDocs.entrySet()) {
            RawBsonDocument tgtDoc = tgtDocs.get(entry.getKey());
            if (tgtDoc == null || !bsonValuesEqual(entry.getValue(), tgtDoc)) {
              return new CheckResult(
                  false, "Mismatch or missing document for _id=" + entry.getKey());
            }
          }
          return new CheckResult(true, "Converged with " + expectedCount + " documents");
        } catch (Exception e) {
          return new CheckResult(false, "Exception during parity check: " + e.getMessage());
        }
      }
    };
  }

  private void assertCollectionParity(String collectionName, int expectedCount) {
    Map<String, RawBsonDocument> srcDocs =
        readRawCollection(sourceClient, sourceDatabase, collectionName);
    Map<String, RawBsonDocument> tgtDocs =
        readRawCollection(targetClient, targetDatabase, collectionName);

    assertWithMessage("Source document count for %s", collectionName)
        .that(srcDocs.size())
        .isEqualTo(expectedCount);

    for (Map.Entry<String, RawBsonDocument> entry : srcDocs.entrySet()) {
      String idKey = entry.getKey();
      RawBsonDocument srcDoc = entry.getValue();
      RawBsonDocument tgtDoc = tgtDocs.get(idKey);
      assertWithMessage("Missing document on target for _id=%s in %s", idKey, collectionName)
          .that(tgtDoc)
          .isNotNull();
      assertWithMessage(
              "BSON mismatch for _id=%s in %s:\nsource=%s\ntarget=%s",
              idKey, collectionName, srcDoc.toJson(), tgtDoc.toJson())
          .that(bsonValuesEqual(srcDoc, tgtDoc))
          .isTrue();
    }

    assertWithMessage("Target document count for %s", collectionName)
        .that(tgtDocs.size())
        .isEqualTo(expectedCount);
  }

  private static Map<String, RawBsonDocument> readRawCollection(
      MongoClient client, String dbName, String collectionName) {
    MongoCollection<RawBsonDocument> col =
        client.getDatabase(dbName).getCollection(collectionName, RawBsonDocument.class);
    Map<String, RawBsonDocument> map = new LinkedHashMap<>();
    for (RawBsonDocument doc : col.find()) {
      String idKey = new BsonDocument("_id", doc.get("_id")).toJson();
      map.put(idKey, doc);
    }
    return map;
  }

  /**
   * Deep BSON value equality check that strictly enforces BSON type tags, IEEE 754 {@code -0.0} vs
   * {@code +0.0}, {@code NaN}, {@code Decimal128}, and {@code BsonBinary} subtypes, while comparing
   * {@code BsonDocument} field sets independent of storage key order.
   */
  private static boolean bsonValuesEqual(BsonValue a, BsonValue b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null || a.getBsonType() != b.getBsonType()) {
      return false;
    }
    if (a.getBsonType() == BsonType.DOUBLE) {
      double da = a.asDouble().getValue();
      double db = b.asDouble().getValue();
      if (Double.isNaN(da) && Double.isNaN(db)) {
        return true;
      }
      return Double.doubleToRawLongBits(da) == Double.doubleToRawLongBits(db);
    }
    if (a.getBsonType() == BsonType.DECIMAL128) {
      Decimal128 da = a.asDecimal128().getValue();
      Decimal128 db = b.asDecimal128().getValue();
      if (da.isNaN() && db.isNaN()) {
        return true;
      }
      return da.equals(db);
    }
    if (a.getBsonType() == BsonType.DOCUMENT) {
      BsonDocument docA = a.asDocument();
      BsonDocument docB = b.asDocument();
      if (!docA.keySet().equals(docB.keySet())) {
        return false;
      }
      for (String key : docA.keySet()) {
        if (!bsonValuesEqual(docA.get(key), docB.get(key))) {
          return false;
        }
      }
      return true;
    }
    if (a.getBsonType() == BsonType.ARRAY) {
      List<BsonValue> listA = a.asArray().getValues();
      List<BsonValue> listB = b.asArray().getValues();
      if (listA.size() != listB.size()) {
        return false;
      }
      for (int i = 0; i < listA.size(); i++) {
        if (!bsonValuesEqual(listA.get(i), listB.get(i))) {
          return false;
        }
      }
      return true;
    }
    return a.equals(b);
  }

  private static List<RawBsonDocument> buildHeterogeneousBsonDocuments() {
    List<BsonDocument> docs = new ArrayList<>();

    // 1. ObjectId _id boundaries + standard ObjectIds
    docs.add(
        new BsonDocument("_id", new BsonObjectId(new ObjectId("000000000000000000000000")))
            .append("kind", new BsonString("oid_min")));
    docs.add(
        new BsonDocument("_id", new BsonObjectId(new ObjectId()))
            .append("kind", new BsonString("oid_random")));
    docs.add(
        new BsonDocument("_id", new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")))
            .append("kind", new BsonString("oid_max")));

    // 2. String _id boundaries (empty, Unicode emoji, high \uFFFF prefix)
    docs.add(
        new BsonDocument("_id", new BsonString("")).append("kind", new BsonString("str_empty")));
    docs.add(
        new BsonDocument("_id", new BsonString("str_emoji_🚀🔥🎉"))
            .append("kind", new BsonString("str_unicode")));
    docs.add(
        new BsonDocument("_id", new BsonString("\uFFFF\uFFFF_high_prefix"))
            .append("kind", new BsonString("str_high")));

    // 3. Numeric _id values: distinct Int32, Int64 (> 2^53), and Double values
    docs.add(
        new BsonDocument("_id", new BsonInt32(Integer.MIN_VALUE))
            .append("kind", new BsonString("int32_min")));
    docs.add(
        new BsonDocument("_id", new BsonInt32(0)).append("kind", new BsonString("int32_zero")));
    docs.add(
        new BsonDocument("_id", new BsonInt32(Integer.MAX_VALUE))
            .append("kind", new BsonString("int32_max")));
    docs.add(
        new BsonDocument("_id", new BsonInt64(Long.MIN_VALUE))
            .append("kind", new BsonString("int64_min")));
    docs.add(
        new BsonDocument("_id", new BsonInt64((1L << 53) + 1L))
            .append("kind", new BsonString("int64_above_float53")));
    docs.add(
        new BsonDocument("_id", new BsonInt64(Long.MAX_VALUE))
            .append("kind", new BsonString("int64_max")));
    docs.add(
        new BsonDocument("_id", new BsonDouble(Math.PI))
            .append("kind", new BsonString("double_pi")));
    docs.add(
        new BsonDocument("_id", new BsonDouble(Double.NEGATIVE_INFINITY))
            .append("kind", new BsonString("double_neg_inf")));
    docs.add(
        new BsonDocument("_id", new BsonDouble(Double.POSITIVE_INFINITY))
            .append("kind", new BsonString("double_pos_inf")));

    // 4. Binary _id values: empty BinData, subtype 0x00, UUID subtype 0x04, user-defined 0x80 &
    // 0xFF
    docs.add(
        new BsonDocument("_id", new BsonBinary(new byte[0]))
            .append("kind", new BsonString("bindata_empty")));
    docs.add(
        new BsonDocument("_id", new BsonBinary(new byte[] {1, 2, 3, 4}))
            .append("kind", new BsonString("bindata_generic")));
    docs.add(
        new BsonDocument(
                "_id",
                new BsonBinary(
                    UUID.fromString("12345678-1234-5678-9abc-123456789abc"),
                    UuidRepresentation.STANDARD))
            .append("kind", new BsonString("bindata_uuid_v4")));
    docs.add(
        new BsonDocument("_id", new BsonBinary((byte) 0x80, new byte[] {(byte) 0xAA, (byte) 0xBB}))
            .append("kind", new BsonString("bindata_user_0x80")));
    docs.add(
        new BsonDocument("_id", new BsonBinary((byte) 0xFF, new byte[] {(byte) 0xFF}))
            .append("kind", new BsonString("bindata_user_0xff")));

    // 5. Composite Document _id values (keys ordered lexicographically for deterministic JSON key)
    docs.add(
        new BsonDocument(
                "_id",
                new BsonDocument("seq", new BsonInt32(1)).append("tenant", new BsonString("acme")))
            .append("kind", new BsonString("composite_doc_1")));
    docs.add(
        new BsonDocument(
                "_id",
                new BsonDocument("seq", new BsonInt32(2)).append("tenant", new BsonString("acme")))
            .append("kind", new BsonString("composite_doc_2")));

    // 6. Rich payload documents covering Decimal128, Date, Timestamp, Boolean, IEEE 754 -0.0/NaN,
    // Regex, Null, MinKey/MaxKey, Array, and nested Document
    docs.add(
        new BsonDocument("_id", new BsonString("temporal_and_decimal_payloads"))
            .append(
                "highPrecisionDecimal",
                new BsonDecimal128(Decimal128.parse("9999999999999999999999999999.123456")))
            .append("datePre1970", new BsonDateTime(-86_400_000L * 365 * 50))
            .append("dateEpochZero", new BsonDateTime(0L))
            .append("datePost1970", new BsonDateTime(1776000000000L))
            .append("tsMin", new BsonTimestamp(1, 1))
            .append("tsNormal", new BsonTimestamp(1776000000, 99))
            .append("boolFalse", BsonBoolean.FALSE)
            .append("boolTrue", BsonBoolean.TRUE));

    docs.add(
        new BsonDocument("_id", new BsonString("special_floats"))
            .append("negZero", new BsonDouble(-0.0d))
            .append("posZero", new BsonDouble(+0.0d))
            .append("nanVal", new BsonDouble(Double.NaN))
            .append("posInf", new BsonDouble(Double.POSITIVE_INFINITY))
            .append("negInf", new BsonDouble(Double.NEGATIVE_INFINITY))
            .append("aboveFloat53", new BsonInt64((1L << 53) + 1L))
            .append("decNan", new BsonDecimal128(Decimal128.NaN))
            .append("decNegZero", new BsonDecimal128(Decimal128.NEGATIVE_ZERO))
            .append("nullField", BsonNull.VALUE)
            .append("regexField", new BsonRegularExpression("^abc.*$", "im"))
            .append("minKeyField", new BsonMinKey())
            .append("maxKeyField", new BsonMaxKey())
            .append(
                "nestedArray",
                new BsonArray(
                    Arrays.asList(
                        new BsonInt32(1),
                        new BsonString("two"),
                        new BsonDocument("deep", BsonBoolean.TRUE)))));

    BsonDocumentCodec codec = new BsonDocumentCodec();
    List<RawBsonDocument> rawDocs = new ArrayList<>(docs.size());
    for (BsonDocument doc : docs) {
      rawDocs.add(new RawBsonDocument(doc, codec));
    }
    return rawDocs;
  }

  private static void dropDatabaseQuietly(MongoClient client, String dbName) {
    if (client != null && dbName != null && !dbName.isEmpty()) {
      try {
        client.getDatabase(dbName).drop();
      } catch (Exception e) {
        LOG.warn("Failed to drop database '{}': {}", dbName, e.getMessage());
      }
    }
  }

  private static void closeClientQuietly(MongoClient client) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception ignored) {
        // Best-effort cleanup during test teardown.
      }
    }
  }
}
