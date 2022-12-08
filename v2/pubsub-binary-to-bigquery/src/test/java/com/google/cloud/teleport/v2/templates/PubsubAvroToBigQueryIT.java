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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubAvroToBigQuery}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubAvroToBigQuery.class)
@RunWith(JUnit4.class)
public final class PubsubAvroToBigQueryIT extends TemplateTestBase {

  private static PubsubResourceManager pubsubResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;
  private Schema avroSchema;
  private com.google.cloud.bigquery.Schema bigQuerySchema;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testId, PROJECT).setCredentials(credentials).build();

    URL avroSchemaResource = Resources.getResource("PubsubAvroToBigQueryIT/avro_schema.avsc");
    artifactClient.uploadArtifact("schema.avsc", avroSchemaResource.getPath());
    avroSchema = new Schema.Parser().parse(avroSchemaResource.openStream());

    bigQuerySchema =
        com.google.cloud.bigquery.Schema.of(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("age", StandardSQLTypeName.INT64),
            Field.of("decimal", StandardSQLTypeName.FLOAT64));
  }

  @After
  public void tearDownClass() {
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testPubsubAvroToBigQuerySimple() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    TopicName topic = pubsubResourceManager.createTopic("input");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-1");

    List<Map<String, Object>> recordMaps =
        List.of(
            Map.of("name", "John", "age", 5, "decimal", 3.3),
            Map.of("name", "Jane", "age", 4, "decimal", 4.4),
            Map.of("name", "Jim", "age", 3, "decimal", 5.5));
    for (Map<String, Object> record : recordMaps) {
      ByteString sendRecord =
          createRecord(
              (String) record.get("name"),
              (Integer) record.get("age"),
              (Double) record.get("decimal"));
      pubsubResourceManager.publish(topic, ImmutableMap.of(), sendRecord);
    }

    TableId people = bigQueryResourceManager.createTable("people", bigQuerySchema);

    // Act
    JobInfo info =
        launchTemplate(
            LaunchConfig.builder(jobName, specPath)
                .addParameter("schemaPath", getGcsPath("schema.avsc"))
                .addParameter("inputSubscription", subscription.toString())
                .addParameter(
                    "outputTableSpec",
                    String.format(
                        "%s:%s.%s", people.getProject(), people.getDataset(), people.getTable()))
                .addParameter("outputTopic", dlqTopic.toString()));
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<TableResult> records = new AtomicReference<>();

    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  TableResult values = bigQueryResourceManager.readTable("people");
                  records.set(values);

                  return values.getTotalRows() >= recordMaps.size();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
    assertThatRecords(records.get()).hasRecords(recordMaps);
  }

  private ByteString createRecord(String name, int age, double decimal) throws IOException {
    GenericRecord record =
        new GenericRecordBuilder(avroSchema)
            .set("name", name)
            .set("age", age)
            .set(
                "decimal",
                ByteBuffer.wrap(
                    new BigDecimal(decimal, MathContext.DECIMAL32)
                        .setScale(17, RoundingMode.HALF_UP)
                        .unscaledValue()
                        .toByteArray()))
            .build();

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(record, encoder);
    encoder.flush();

    return ByteString.copyFrom(output.toByteArray());
  }
}
