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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.google.common.collect.ImmutableList;
import com.google.firestore.admin.v1.CreateDatabaseRequest;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.Database.DataAccessMode;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import com.google.firestore.admin.v1.DeleteDatabaseRequest;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.dataflow.DirectRunnerClient;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
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
import org.bson.codecs.Codec;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared static BSON document fixtures, endpoint provisioning, pipeline configuration, and parity
 * verification helpers for {@link MongoDbToMongoDbIT}.
 */
final class MongoDbTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbTestHelper.class);
  private static final String MONGO_IMAGE_TAG = "7.0";
  private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();
  private static final Codec<Document> DOCUMENT_CODEC =
      MongoClientSettings.getDefaultCodecRegistry().get(Document.class);

  // ---------------------------------------------------------------------------
  // 1. Static Heterogeneous BSON Document Fixtures (Immutable RawBsonDocuments)
  // ---------------------------------------------------------------------------

  static final RawBsonDocument OID_MIN_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonObjectId(new ObjectId("000000000000000000000000")))
              .append("kind", new BsonString("oid_min")));

  static final RawBsonDocument OID_STANDARD_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011")))
              .append("kind", new BsonString("oid_standard")));

  static final RawBsonDocument OID_MAX_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonObjectId(new ObjectId("ffffffffffffffffffffffff")))
              .append("kind", new BsonString("oid_max")));

  static final RawBsonDocument STR_EMPTY_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("")).append("kind", new BsonString("str_empty")));

  static final RawBsonDocument STR_UNICODE_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("str_emoji_🚀🔥🎉"))
              .append("kind", new BsonString("str_unicode")));

  static final RawBsonDocument STR_HIGH_PREFIX_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("\uFFFF\uFFFF_high_prefix"))
              .append("kind", new BsonString("str_high")));

  static final RawBsonDocument INT32_MIN_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt32(Integer.MIN_VALUE))
              .append("kind", new BsonString("int32_min")));

  static final RawBsonDocument INT32_ZERO_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt32(0)).append("kind", new BsonString("int32_zero")));

  static final RawBsonDocument INT32_MAX_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt32(Integer.MAX_VALUE))
              .append("kind", new BsonString("int32_max")));

  static final RawBsonDocument INT64_MIN_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt64(Long.MIN_VALUE))
              .append("kind", new BsonString("int64_min")));

  static final RawBsonDocument INT64_ABOVE_FLOAT53_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt64((1L << 53) + 1L))
              .append("kind", new BsonString("int64_above_float53")));

  static final RawBsonDocument INT64_MAX_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonInt64(Long.MAX_VALUE))
              .append("kind", new BsonString("int64_max")));

  static final RawBsonDocument DOUBLE_PI_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonDouble(Math.PI))
              .append("kind", new BsonString("double_pi")));

  static final RawBsonDocument DOUBLE_NEG_INF_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonDouble(Double.NEGATIVE_INFINITY))
              .append("kind", new BsonString("double_neg_inf")));

  static final RawBsonDocument DOUBLE_POS_INF_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonDouble(Double.POSITIVE_INFINITY))
              .append("kind", new BsonString("double_pos_inf")));

  static final RawBsonDocument BINDATA_EMPTY_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonBinary(new byte[0]))
              .append("kind", new BsonString("bindata_empty")));

  static final RawBsonDocument BINDATA_GENERIC_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonBinary(new byte[] {1, 2, 3, 4}))
              .append("kind", new BsonString("bindata_generic")));

  static final RawBsonDocument BINDATA_UUID_V4_DOC =
      rawDoc(
          new BsonDocument(
                  "_id",
                  new BsonBinary(
                      UUID.fromString("12345678-1234-5678-9abc-123456789abc"),
                      UuidRepresentation.STANDARD))
              .append("kind", new BsonString("bindata_uuid_v4")));

  static final RawBsonDocument BINDATA_USER_0X80_DOC =
      rawDoc(
          new BsonDocument(
                  "_id", new BsonBinary((byte) 0x80, new byte[] {(byte) 0xAA, (byte) 0xBB}))
              .append("kind", new BsonString("bindata_user_0x80")));

  static final RawBsonDocument BINDATA_USER_0XFF_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonBinary((byte) 0xFF, new byte[] {(byte) 0xFF}))
              .append("kind", new BsonString("bindata_user_0xff")));

  static final RawBsonDocument COMPOSITE_ID_DOC_1 =
      rawDoc(
          new BsonDocument(
                  "_id",
                  new BsonDocument("seq", new BsonInt32(1))
                      .append("tenant", new BsonString("acme")))
              .append("kind", new BsonString("composite_doc_1")));

  static final RawBsonDocument COMPOSITE_ID_DOC_2 =
      rawDoc(
          new BsonDocument(
                  "_id",
                  new BsonDocument("seq", new BsonInt32(2))
                      .append("tenant", new BsonString("acme")))
              .append("kind", new BsonString("composite_doc_2")));

  static final RawBsonDocument TEMPORAL_AND_DECIMAL_PAYLOAD_DOC =
      rawDoc(
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

  static final RawBsonDocument SPECIAL_FLOATS_AND_NESTED_PAYLOAD_DOC =
      rawDoc(
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
                      ImmutableList.of(
                          new BsonInt32(1),
                          new BsonString("two"),
                          new BsonDocument("deep", BsonBoolean.TRUE)))));

  static final ImmutableList<RawBsonDocument> HETEROGENEOUS_BSON_DOCS =
      ImmutableList.of(
          OID_MIN_DOC,
          OID_STANDARD_DOC,
          OID_MAX_DOC,
          STR_EMPTY_DOC,
          STR_UNICODE_DOC,
          STR_HIGH_PREFIX_DOC,
          INT32_MIN_DOC,
          INT32_ZERO_DOC,
          INT32_MAX_DOC,
          INT64_MIN_DOC,
          INT64_ABOVE_FLOAT53_DOC,
          INT64_MAX_DOC,
          DOUBLE_PI_DOC,
          DOUBLE_NEG_INF_DOC,
          DOUBLE_POS_INF_DOC,
          BINDATA_EMPTY_DOC,
          BINDATA_GENERIC_DOC,
          BINDATA_UUID_V4_DOC,
          BINDATA_USER_0X80_DOC,
          BINDATA_USER_0XFF_DOC,
          COMPOSITE_ID_DOC_1,
          COMPOSITE_ID_DOC_2,
          TEMPORAL_AND_DECIMAL_PAYLOAD_DOC,
          SPECIAL_FLOATS_AND_NESTED_PAYLOAD_DOC);

  // ---------------------------------------------------------------------------
  // 2. Static CDC, UDF, and DLQ Document Fixtures & Constants
  // ---------------------------------------------------------------------------

  static final BsonBinary OVERLAP_DELETED_UUID_ID =
      new BsonBinary(
          UUID.fromString("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"), UuidRepresentation.STANDARD);

  static final BsonBinary STREAMING_DELETED_UUID_ID =
      new BsonBinary(
          UUID.fromString("11111111-2222-4333-8444-555555555555"), UuidRepresentation.STANDARD);

  static final BsonBinary STREAMING_SURVIVING_UUID_ID =
      new BsonBinary(
          UUID.fromString("66666666-7777-4888-9999-aaaaaaaaaaaa"), UuidRepresentation.STANDARD);

  static final RawBsonDocument STREAM_PRE_T0_DOC_1 =
      rawDoc(new BsonDocument("_id", new BsonString("pre_t0_1")).append("score", new BsonInt32(5)));

  static final RawBsonDocument STREAM_PRE_T0_DOC_2 =
      rawDoc(new BsonDocument("_id", new BsonString("pre_t0_2")).append("score", new BsonInt32(6)));

  static final RawBsonDocument STREAM_POST_T0_VALID_DOC_1 =
      rawDoc(
          new BsonDocument("_id", new BsonString("post_t0_valid_1"))
              .append("score", new BsonInt32(10)));

  static final RawBsonDocument STREAM_POST_T0_FILTERED_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("post_t0_filtered"))
              .append("filterOut", BsonBoolean.TRUE));

  static final RawBsonDocument STREAM_POST_T0_POISON_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("post_t0_poison"))
              .append("poison", BsonBoolean.TRUE)
              .append("secret", new BsonString("keep_me")));

  static final RawBsonDocument STREAM_POST_T0_VALID_DOC_2 =
      rawDoc(
          new BsonDocument("_id", new BsonString("post_t0_valid_2"))
              .append("score", new BsonInt32(25)));

  static final RawBsonDocument DLQ_VALID_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("dlq_valid_doc"))
              .append("score", new BsonInt32(7)));

  static final RawBsonDocument DLQ_UDF_POISON_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("dlq_udf_poison_doc"))
              .append("poison", BsonBoolean.TRUE)
              .append("payload", new BsonString("preserve_original_in_retryable_dlq")));

  /**
   * Cloud Firestore Enterprise rejects reserved {@code __fs_*__} field names per statement with
   * {@code INVALID_ARGUMENT} (MongoDB error code 2), which {@code WriteBatchesFn} routes to
   * permanent DLQ.
   */
  static final RawBsonDocument DLQ_PERMANENT_REJECT_FIRESTORE_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("dlq_permanent_reject_doc"))
              .append("score", new BsonInt32(-5))
              .append("payload", new BsonString("preserve_in_permanent_dlq"))
              .append("__fs_invalid__", new BsonString("reserved_field_rejected_by_firestore")));

  static final RawBsonDocument DLQ_PERMANENT_REJECT_SCHEMA_DOC =
      rawDoc(
          new BsonDocument("_id", new BsonString("dlq_permanent_reject_doc"))
              .append("score", new BsonInt32(-5))
              .append("payload", new BsonString("preserve_in_permanent_dlq")));

  static final String TRANSFORM_UDF_SCRIPT =
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

  // ---------------------------------------------------------------------------
  // 3. Environment Provisioning, Launch Config, and Dataset Builders
  // ---------------------------------------------------------------------------

  /**
   * Encapsulates provisioned source/target endpoints, pipeline configuration context, and parity
   * checks for an integration test run.
   */
  static final class TestEnvironment {
    final MongoDBResourceManager sourceMongoManager;
    final MongoDBResourceManager targetMongoManager;
    final String sourceUri;
    final String targetUri;
    final String sourceDatabase;
    final String targetDatabase;
    final MongoClient sourceClient;
    final MongoClient targetClient;
    private final FirestoreAdminClient firestoreAdminClient;
    private final String firestoreDatabaseResourceName;
    private final String defaultJobName;
    private final String specPath;
    private final PipelineLauncher launcher;
    private final GcsResourceManager gcsClient;
    private final Function<String, String> gcsPathResolver;
    private final TemporaryFolder tempFolder;

    private TestEnvironment(
        MongoDBResourceManager sourceMongoManager,
        MongoDBResourceManager targetMongoManager,
        FirestoreAdminClient firestoreAdminClient,
        String firestoreDatabaseResourceName,
        String sourceUri,
        String targetUri,
        String sourceDatabase,
        String targetDatabase,
        MongoClient sourceClient,
        MongoClient targetClient,
        String defaultJobName,
        String specPath,
        PipelineLauncher launcher,
        GcsResourceManager gcsClient,
        Function<String, String> gcsPathResolver,
        TemporaryFolder tempFolder) {
      this.sourceMongoManager = sourceMongoManager;
      this.targetMongoManager = targetMongoManager;
      this.firestoreAdminClient = firestoreAdminClient;
      this.firestoreDatabaseResourceName = firestoreDatabaseResourceName;
      this.sourceUri = sourceUri;
      this.targetUri = targetUri;
      this.sourceDatabase = sourceDatabase;
      this.targetDatabase = targetDatabase;
      this.sourceClient = sourceClient;
      this.targetClient = targetClient;
      this.defaultJobName = defaultJobName;
      this.specPath = specPath;
      this.launcher = launcher;
      this.gcsClient = gcsClient;
      this.gcsPathResolver = gcsPathResolver;
      this.tempFolder = tempFolder;
    }

    MongoDatabase sourceDb() {
      return sourceClient.getDatabase(sourceDatabase);
    }

    MongoDatabase targetDb() {
      return targetClient.getDatabase(targetDatabase);
    }

    boolean isFirestoreTarget() {
      return firestoreDatabaseResourceName != null;
    }

    LaunchConfig.Builder baseLaunchConfig() throws IOException {
      return baseLaunchConfig(defaultJobName);
    }

    LaunchConfig.Builder baseLaunchConfig(String jobName) throws IOException {
      LaunchConfig.Builder builder =
          LaunchConfig.builder(jobName, specPath)
              .addParameter("sourceUri", sourceUri)
              .addParameter("targetUri", targetUri)
              .addParameter("sourceDatabase", sourceDatabase)
              .addParameter("targetDatabase", targetDatabase)
              .addParameter("initialWriteRatePerWorker", "0");
      if (isDirectRunner(launcher)) {
        Path localTemp =
            tempFolder
                .newFolder("beam-temp-" + UUID.randomUUID().toString().substring(0, 6))
                .toPath();
        builder.addParameter("tempLocation", localTemp.toString());
      }
      return builder;
    }

    String stageUdfFile(String relativePath, String scriptContent) throws IOException {
      if (isDirectRunner(launcher) || gcsClient == null) {
        Path localUdf =
            tempFolder
                .newFile("transform_" + UUID.randomUUID().toString().substring(0, 6) + ".js")
                .toPath();
        Files.writeString(localUdf, scriptContent, StandardCharsets.UTF_8);
        return localUdf.toString();
      }
      gcsClient.createArtifact(relativePath, scriptContent);
      return gcsPathResolver.apply(relativePath);
    }

    String resolveDlqDirectory(String name) throws IOException {
      if (isDirectRunner(launcher) || gcsClient == null) {
        return tempFolder.newFolder(name).toPath().toString();
      }
      return gcsPathResolver.apply(name);
    }

    String readDlqContents(String gcsPrefix, String resolvedDlqDir, String tier)
        throws IOException {
      if (isDirectRunner(launcher) || gcsClient == null) {
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

    RawBsonDocument prepareBackfillDlqTargetAndGetRejectDoc(String dlqCollection) {
      if (isFirestoreTarget()) {
        return DLQ_PERMANENT_REJECT_FIRESTORE_DOC;
      }
      targetDb()
          .createCollection(
              dlqCollection,
              new CreateCollectionOptions()
                  .validationOptions(
                      new ValidationOptions().validator(Filters.gte("doubledScore", 0))));
      return DLQ_PERMANENT_REJECT_SCHEMA_DOC;
    }

    BsonTimestamp captureSourceClusterTime() {
      Document hello = sourceDb().runCommand(new Document("hello", 1));
      BsonTimestamp opTime = hello.get("operationTime", BsonTimestamp.class);
      assertThat(opTime).isNotNull();
      return opTime;
    }

    ConditionCheck collectionParityCheck(String collectionName, int expectedCount) {
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

    void assertCollectionParity(String collectionName, int expectedCount) {
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

    void tearDown() {
      dropDatabaseQuietly(sourceClient, sourceDatabase);
      if (!isFirestoreTarget()) {
        dropDatabaseQuietly(targetClient, targetDatabase);
      }
      closeClientQuietly(sourceClient);
      closeClientQuietly(targetClient);
      deleteFirestoreDatabaseQuietly(firestoreAdminClient, firestoreDatabaseResourceName);
      ResourceManagerUtils.cleanResources(sourceMongoManager, targetMongoManager);
    }
  }

  static TestEnvironment setUpEnvironment(
      String testId,
      String testName,
      String specPath,
      String project,
      String region,
      Credentials credentials,
      PipelineLauncher launcher,
      GcsResourceManager gcsClient,
      Function<String, String> gcsPathResolver,
      TemporaryFolder tempFolder)
      throws IOException {
    String uniqueSuffix =
        testId.replaceAll("[^a-zA-Z0-9]", "").toLowerCase()
            + "_"
            + UUID.randomUUID().toString().substring(0, 6);

    MongoDBResourceManager sourceMongoManager = null;
    MongoDBResourceManager targetMongoManager = null;
    FirestoreAdminClient firestoreAdminClient = null;
    String firestoreDatabaseResourceName = null;
    MongoClient sourceClient = null;
    MongoClient targetClient = null;

    try {
      String sourceUri;
      String sourceDatabase;
      String customSourceUri =
          TestProperties.getProperty("sourceMongoUri", "", TestProperties.Type.PROPERTY);
      if (customSourceUri != null && !customSourceUri.isEmpty()) {
        sourceUri = customSourceUri;
        sourceDatabase = "it_src_" + uniqueSuffix;
      } else {
        sourceMongoManager =
            MongoDBResourceManager.builder(testName).setContainerImageTag(MONGO_IMAGE_TAG).build();
        sourceUri = sourceMongoManager.getUri();
        sourceDatabase = sourceMongoManager.getDatabaseName();
      }

      String targetUri;
      String targetDatabase;
      String customTargetUri =
          TestProperties.getProperty("targetMongoUri", "", TestProperties.Type.PROPERTY);
      boolean useTestcontainersTarget =
          Boolean.parseBoolean(
              TestProperties.getProperty(
                  "useTestcontainersTarget", "false", TestProperties.Type.PROPERTY));

      if (customTargetUri != null && !customTargetUri.isEmpty()) {
        targetUri = customTargetUri;
        targetDatabase = "it_tgt_" + uniqueSuffix;
      } else if (useTestcontainersTarget) {
        targetMongoManager =
            MongoDBResourceManager.builder(testName + "_target")
                .setContainerImageTag(MONGO_IMAGE_TAG)
                .build();
        targetUri = targetMongoManager.getUri();
        targetDatabase = targetMongoManager.getDatabaseName();
      } else {
        targetDatabase = "dst-" + UUID.randomUUID();
        String resourceName = "projects/" + project + "/databases/" + targetDatabase;
        FirestoreAdminSettings.Builder settingsBuilder = FirestoreAdminSettings.newBuilder();
        if (credentials != null) {
          settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        firestoreAdminClient = FirestoreAdminClient.create(settingsBuilder.build());
        LOG.info(
            "Creating Firestore Enterprise MongoDB-compatible database '{}' in {}...",
            resourceName,
            region);
        Database createdDb =
            createFirestoreDatabase(firestoreAdminClient, project, region, targetDatabase);
        firestoreDatabaseResourceName = resourceName;
        if (createdDb.getUid().isEmpty()) {
          createdDb = firestoreAdminClient.getDatabase(resourceName);
        }
        LOG.info(
            "Provisioned Firestore Enterprise database '{}' (uid={}, location={})",
            targetDatabase,
            createdDb.getUid(),
            createdDb.getLocationId());
        targetUri = buildFirestoreMongoUri(createdDb, targetDatabase, credentials);
      }

      sourceClient = MongoDbTransforms.createMongoClient(sourceUri);
      targetClient = MongoDbTransforms.createMongoClient(targetUri);

      return new TestEnvironment(
          sourceMongoManager,
          targetMongoManager,
          firestoreAdminClient,
          firestoreDatabaseResourceName,
          sourceUri,
          targetUri,
          sourceDatabase,
          targetDatabase,
          sourceClient,
          targetClient,
          testName,
          specPath,
          launcher,
          gcsClient,
          gcsPathResolver,
          tempFolder);
    } catch (RuntimeException | IOException e) {
      closeClientQuietly(sourceClient);
      closeClientQuietly(targetClient);
      deleteFirestoreDatabaseQuietly(firestoreAdminClient, firestoreDatabaseResourceName);
      ResourceManagerUtils.cleanResources(sourceMongoManager, targetMongoManager);
      throw e;
    }
  }

  static boolean isDirectRunner(PipelineLauncher launcher) {
    return launcher instanceof DirectRunnerClient;
  }

  static List<RawBsonDocument> createSecondaryDocs(int count) {
    List<RawBsonDocument> docs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      docs.add(
          new RawBsonDocument(
              new Document("_id", "sec_" + i)
                  .append("index", i)
                  .append("group", i % 3)
                  .append("label", "secondary_collection_doc_" + i),
              DOCUMENT_CODEC));
    }
    return docs;
  }

  static List<Document> createOverlapBaselineDocs(int count) {
    List<Document> docs = new ArrayList<>(count + 1);
    for (int i = 0; i < count; i++) {
      docs.add(new Document("_id", "doc_" + i).append("v", 1).append("stage", "baseline_" + i));
    }
    docs.add(
        new Document("_id", OVERLAP_DELETED_UUID_ID).append("v", 1).append("stage", "uuid_base"));
    return docs;
  }

  static List<Document> createBurstDocs(int count) {
    List<Document> docs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      docs.add(new Document("_id", "burst_" + i).append("v", 1).append("idx", i));
    }
    return docs;
  }

  static void createCollectionWithPreAndPostImages(MongoDatabase db, String collectionName) {
    db.createCollection(collectionName);
    db.runCommand(
        new Document("collMod", collectionName)
            .append("changeStreamPreAndPostImages", new Document("enabled", true)));
  }

  static BsonTimestamp advanceAndCaptureClusterTime(MongoDatabase db) {
    String tickCol = "_clock_tick_" + UUID.randomUUID().toString().substring(0, 6);
    db.getCollection(tickCol).insertOne(new Document("_id", "tick"));
    db.getCollection(tickCol).drop();
    Document hello = db.runCommand(new Document("hello", 1));
    return hello.get("operationTime", BsonTimestamp.class);
  }

  // ---------------------------------------------------------------------------
  // 4. Condition Checks and BSON Parity Verification
  // ---------------------------------------------------------------------------

  static ConditionCheck udfStreamConvergedCheck(MongoCollection<Document> tgtStreamCol) {
    return new ConditionCheck() {
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
    };
  }

  static void assertSpecialFloatsPreserved(MongoDatabase targetDb, String collectionName) {
    MongoCollection<RawBsonDocument> tgtBsonCol =
        targetDb.getCollection(collectionName, RawBsonDocument.class);
    RawBsonDocument negZeroDoc =
        tgtBsonCol.find(new BsonDocument("_id", new BsonString("special_floats"))).first();
    assertThat(negZeroDoc).isNotNull();
    assertThat(Double.doubleToRawLongBits(negZeroDoc.getDouble("negZero").getValue()))
        .isEqualTo(Double.doubleToRawLongBits(-0.0d));
    assertThat(Double.isNaN(negZeroDoc.getDouble("nanVal").getValue())).isTrue();
    assertThat(negZeroDoc.getInt64("aboveFloat53").getValue()).isEqualTo((1L << 53) + 1L);
  }

  /**
   * Deep BSON value equality check that strictly enforces BSON type tags, IEEE 754 {@code -0.0} vs
   * {@code +0.0}, {@code NaN}, {@code Decimal128}, and {@code BsonBinary} subtypes, while comparing
   * {@code BsonDocument} field sets independent of storage key order.
   */
  static boolean bsonValuesEqual(BsonValue a, BsonValue b) {
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

  // ---------------------------------------------------------------------------
  // Private Implementation Helpers
  // ---------------------------------------------------------------------------

  private static RawBsonDocument rawDoc(BsonDocument doc) {
    return new RawBsonDocument(doc, BSON_DOCUMENT_CODEC);
  }

  private static Database createFirestoreDatabase(
      FirestoreAdminClient adminClient, String project, String region, String targetDatabase)
      throws IOException {
    try {
      return adminClient
          .createDatabaseAsync(
              CreateDatabaseRequest.newBuilder()
                  .setParent("projects/" + project)
                  .setDatabaseId(targetDatabase)
                  .setDatabase(
                      Database.newBuilder()
                          .setName(targetDatabase)
                          .setType(DatabaseType.FIRESTORE_NATIVE)
                          .setDatabaseEdition(DatabaseEdition.ENTERPRISE)
                          .setMongodbCompatibleDataAccessMode(
                              DataAccessMode.DATA_ACCESS_MODE_ENABLED)
                          .setLocationId(region)
                          .build())
                  .build())
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while creating Firestore database " + targetDatabase, e);
    } catch (ExecutionException e) {
      throw new IOException("Failed to create Firestore database " + targetDatabase, e);
    }
  }

  private static String buildFirestoreMongoUri(
      Database db, String targetDatabase, Credentials credentials) throws IOException {
    String encodedToken = URLEncoder.encode(getAccessToken(credentials), StandardCharsets.UTF_8);
    return String.format(
        "mongodb://access_token:%s@%s.%s.firestore.goog:443/%s"
            + "?loadBalanced=true&tls=true&retryWrites=false"
            + "&authMechanism=PLAIN&authSource=$external",
        encodedToken, db.getUid(), db.getLocationId(), targetDatabase);
  }

  private static void deleteFirestoreDatabaseQuietly(
      FirestoreAdminClient adminClient, String databaseResourceName) {
    if (adminClient == null) {
      return;
    }
    try (adminClient) {
      if (databaseResourceName != null) {
        // Invoke the unary DeleteDatabase RPC directly rather than polling deleteDatabaseAsync().
        // Firestore deletes the database metadata immediately, whereas its background storage-GC
        // LRO stays open past GAX's 5-minute poll timeout and throws CancellationException.
        adminClient
            .deleteDatabaseCallable()
            .call(DeleteDatabaseRequest.newBuilder().setName(databaseResourceName).build());
        LOG.info("Deleted Firestore Enterprise database '{}'", databaseResourceName);
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to delete Firestore Enterprise database '{}': {}",
          databaseResourceName,
          e.getMessage());
    }
  }

  private static String getAccessToken(Credentials credentials) throws IOException {
    GoogleCredentials googleCredentials =
        ((GoogleCredentials) credentials)
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    googleCredentials.refreshIfExpired();
    return googleCredentials.getAccessToken().getTokenValue();
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

  private MongoDbTestHelper() {}
}
