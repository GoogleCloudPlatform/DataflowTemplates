/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.mongodb.MongoDBResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link MongoDbToBigQuery} (MongoDB_to_BigQuery).
 *
 * <p>Example Usage:
 *
 * <pre>
 * # Set the pipeline vars
 * export PROJECT=&lt;project id&gt;
 * export REGION=&lt;dataflow region&gt;
 * export TEMPLATE_MODULE=v2/mongodb-to-googlecloud
 * export ARTIFACT_BUCKET=&lt;bucket name&gt;
 * export HOST_IP=&lt;your host ip&gt;
 *
 * # To set the host ip to the default external ip
 * export HOST_IP=$(hostname -I | awk '{print $1}')
 *
 * # To set the gcloud project credential
 * gcloud config set project ${PROJECT}
 * DT_IT_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
 *
 * # Build and run integration test
 * mvn verify
 *   -pl v2/mongodb-to-googlecloud \
 *   -am \
 *   -Dtest="MongoDbToBigQueryIT" \
 *   -Dproject=${PROJECT} \
 *   -Dregion=${REGION} \
 *   -DartifactBucket=${ARTIFACT_BUCKET} \
 *   -DhostIp=${HOST_IP} \
 *   -Djib.skip \
 *   -DfailIfNoTests=false
 *
 * # Optional mvn flags
 *   -DnumDocs=&lt;numDocs&gt;                # Sets the number of documents to store in MongoDB
 *   -DnumFields=&lt;numFields&gt;            # Sets the number of fields to include in each document
 *   -DmaxEntryLength=&lt;maxEntryLength&gt;  # Sets the maximum length of each document field
 * </pre>
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(MongoDbToBigQuery.class)
@RunWith(JUnit4.class)
public final class MongoDbToBigQueryIT extends TemplateTestBase {
  private static final String MONGO_URI = "mongoDbUri";
  private static final String MONGO_DB = "database";
  private static final String MONGO_COLLECTION = "collection";
  private static final String BIGQUERY_TABLE = "outputTableSpec";
  private static final String USER_OPTION = "userOption";

  private static final String MONGO_DB_ID = "_id";

  private MongoDBResourceManager mongoDbClient;
  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() {
    mongoDbClient = MongoDBResourceManager.builder(testName).build();
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(mongoDbClient, bigQueryClient);
  }

  @Test
  public void testMongoDbToBigQuery() throws IOException {
    // Arrange
    String collectionName = testName;
    List<Document> mongoDocuments = generateDocuments();
    mongoDbClient.insertDocuments(collectionName, mongoDocuments);

    String bqTable = testName;
    String udfFileName = "transform.js";
    gcsClient.createArtifact(
        "input/" + udfFileName,
        "function transform(inJson) {\n"
            + "    var outJson = JSON.parse(inJson);\n"
            + "    outJson.udf = \"out\";\n"
            + "    return JSON.stringify(outJson);\n"
            + "}");
    List<Field> bqSchemaFields = new ArrayList<>();
    bqSchemaFields.add(Field.of("timestamp", StandardSQLTypeName.TIMESTAMP));
    mongoDocuments
        .get(0)
        .forEach((key, val) -> bqSchemaFields.add(Field.of(key, StandardSQLTypeName.STRING)));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryClient.createDataset(REGION);
    TableId table = bigQueryClient.createTable(bqTable, bqSchema);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(MONGO_URI, mongoDbClient.getUri())
            .addParameter(MONGO_DB, mongoDbClient.getDatabaseName())
            .addParameter(MONGO_COLLECTION, collectionName)
            .addParameter(BIGQUERY_TABLE, toTableSpecLegacy(table))
            .addParameter(USER_OPTION, "FLATTEN")
            .addParameter("javascriptDocumentTransformGcsPath", getGcsPath("input/" + udfFileName))
            .addParameter("javascriptDocumentTransformFunctionName", "transform");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, table).setMinRows(1).build());

    // Assert
    assertThatResult(result).meetsConditions();

    Map<String, JSONObject> mongoMap = new HashMap<>();
    mongoDocuments.forEach(
        mongoDocument -> {
          JSONObject mongoDbJson = new JSONObject(mongoDocument.toJson());
          String mongoId = mongoDbJson.getJSONObject(MONGO_DB_ID).getString("$oid");
          mongoDbJson.put("udf", "out");
          mongoDbJson.put(MONGO_DB_ID, mongoId);
          mongoMap.put(mongoId, mongoDbJson);
        });

    TableResult tableRows = bigQueryClient.readTable(bqTable);
    tableRows
        .getValues()
        .forEach(
            row ->
                row.forEach(
                    val -> {
                      JSONObject bigQueryJson = new JSONObject(val.getStringValue());
                      assertTrue(bigQueryJson.has("timestamp"));

                      bigQueryJson.remove("timestamp");
                      String bigQueryId = bigQueryJson.getString(MONGO_DB_ID);
                      assertTrue(mongoMap.get(bigQueryId).similar(bigQueryJson));
                    }));
  }

  private static List<Document> generateDocuments() {
    int numDocuments =
        Integer.parseInt(
            TestProperties.getProperty("numDocs", "100", TestProperties.Type.PROPERTY));
    int numFields =
        Integer.parseInt(
            TestProperties.getProperty("numFields", "200", TestProperties.Type.PROPERTY));
    int maxEntryLength =
        Integer.parseInt(
            TestProperties.getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY));
    List<Document> mongoDocuments = new ArrayList<>();

    List<String> mongoDocumentKeys = new ArrayList<>();
    for (int j = 0; j < numFields; j++) {

      // Generate unique field name
      String randomFieldName = null;
      while (randomFieldName == null || mongoDocumentKeys.contains(randomFieldName.toLowerCase())) {
        randomFieldName =
            RandomStringUtils.randomAlphabetic(2)
                + RandomStringUtils.randomAlphanumeric(0, maxEntryLength - 2);
      }
      mongoDocumentKeys.add(randomFieldName.toLowerCase());
    }
    mongoDocumentKeys.add("udf");
    mongoDocumentKeys.add("nullonly");

    for (int i = 0; i < numDocuments; i++) {
      Document randomDocument = new Document().append(MONGO_DB_ID, new ObjectId());

      for (int j = 0; j < numFields; j++) {
        randomDocument.append(
            mongoDocumentKeys.get(j), RandomStringUtils.randomAlphanumeric(0, 20));
      }
      randomDocument.append("udf", "in");
      randomDocument.append("nullonly", null);

      mongoDocuments.add(randomDocument);
    }

    return mongoDocuments;
  }
}
