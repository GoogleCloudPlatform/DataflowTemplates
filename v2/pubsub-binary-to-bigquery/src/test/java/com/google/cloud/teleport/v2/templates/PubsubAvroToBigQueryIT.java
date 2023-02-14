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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
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
// SkipDirectRunnerTest: PubsubIO doesn't trigger panes on the DirectRunner.
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubsubAvroToBigQuery.class)
@RunWith(JUnit4.class)
public final class PubsubAvroToBigQueryIT extends TemplateTestBase {

  private Schema avroSchema;
  private com.google.cloud.bigquery.Schema bigQuerySchema;

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
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
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testPubsubAvroToBigQuerySimple() throws IOException {
    // Arrange
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
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("schemaPath", getGcsPath("schema.avsc"))
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("outputTableSpec", toTableSpec(people))
                .addParameter("outputTopic", dlqTopic.toString()));
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, people).setMinRows(1).build());

    // Assert
    assertThatResult(result).meetsConditions();
    assertThatRecords(bigQueryResourceManager.readTable(people)).hasRecords(recordMaps);
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
