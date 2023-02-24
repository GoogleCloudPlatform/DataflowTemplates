/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.syndeo.dlq.pubsub;

import static org.junit.Assert.assertEquals;

import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.dlq.pubsub.PubsubDlqSchemaTransformProvider.PubsubDlqSchemaTransform;
import com.google.cloud.syndeo.dlq.pubsub.PubsubDlqSchemaTransformProvider.PubsubDlqSchemaTransform.ConvertRowToMessage;
import com.google.cloud.syndeo.dlq.pubsub.PubsubDlqSchemaTransformProvider.PubsubDlqWriteConfiguration;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateLoadTest.class)
@RunWith(JUnit4.class)
public class PubsubDlqSchemaTransformProviderTest {
  private static final String TOPIC = "projects/project/topics/topic";

  private static final PipelineOptions OPTIONS = PipelineOptionsFactory.create();

  private static final Schema LOCAL_TEST_INPUT_SCHEMA =
      Schema.builder().addStringField("name").build();

  private static Row generateRow() {
    return Row.withSchema(LOCAL_TEST_INPUT_SCHEMA).addValue("testData").build();
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPubsubDlqSchemaTransformIsFound() {
    Collection<SchemaTransformProvider> providers = ProviderUtil.getProviders();
    assertEquals(
        1,
        providers.stream()
            .filter(
                (provider) ->
                    provider
                        .identifier()
                        .equals("syndeo:schematransform:com.google.cloud:pubsub_dlq_write:v1"))
            .count());
  }

  @Test
  public void testPubsubDlqWriteSuccess() {
    TopicPath topicPath = PubsubClient.topicPathFromPath(TOPIC);
    List<OutgoingMessage> outgoing =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(generateRow().toString(true)))
                    .build(),
                -9223372036854775L,
                null));
    PubsubTestClientFactory pubsubFactory =
        PubsubTestClient.createFactoryForPublish(topicPath, outgoing, ImmutableList.of());

    PCollectionRowTuple.of(
            "errors",
            pipeline.apply(Create.of(generateRow())).setRowSchema(LOCAL_TEST_INPUT_SCHEMA))
        .apply(
            transform(PubsubDlqWriteConfiguration.create(TOPIC))
                .withPubsubClientFactory(pubsubFactory)
                .buildTransform());

    pipeline.run(OPTIONS);
  }

  @Test
  public void testConvertRowToMessageNoTruncationSuccess() throws IOException {
    Row expected_row = Row.withSchema(LOCAL_TEST_INPUT_SCHEMA).addValue("testData").build();
    PAssert.that(
            "The output message should be the same as input.",
            pipeline
                .apply(Create.of(generateRow()))
                .setRowSchema(LOCAL_TEST_INPUT_SCHEMA)
                .apply(ParDo.of(new ConvertRowToMessage())))
        .containsInAnyOrder(
            new PubsubMessage(
                expected_row.toString(true).getBytes(StandardCharsets.UTF_8), Map.of()));
    pipeline.run(OPTIONS);
  }

  @Test
  public void testConvertRowToMessageTruncationSuccess() throws IOException {
    // Create input & output char arrays with input being twice the acceptable size.
    char[] input_chars = new char[2 * PubsubDlqSchemaTransform.PUBSUB_MESSAGE_SIZE];
    char[] output_chars = new char[PubsubDlqSchemaTransform.PUBSUB_MESSAGE_SIZE];
    Arrays.fill(input_chars, 'a');
    Arrays.fill(output_chars, 'a');
    Row input_row =
        Row.withSchema(LOCAL_TEST_INPUT_SCHEMA).addValue(new String(input_chars)).build();
    Row expected_row =
        Row.withSchema(LOCAL_TEST_INPUT_SCHEMA).addValue(new String(output_chars)).build();
    // While the input char array was of acceptable size, after converting it to row, the final
    // byte array would be just over the limit. Hence, we need to truncate it again.
    byte[] expected_bytes = new byte[PubsubDlqSchemaTransform.PUBSUB_MESSAGE_SIZE];
    System.arraycopy(
        expected_row.toString(true).getBytes(StandardCharsets.UTF_8),
        0,
        expected_bytes,
        0,
        PubsubDlqSchemaTransform.PUBSUB_MESSAGE_SIZE);

    PAssert.that(
            "The output message should be of the truncated input size.",
            pipeline
                .apply(Create.of(input_row))
                .setRowSchema(LOCAL_TEST_INPUT_SCHEMA)
                .apply(ParDo.of(new ConvertRowToMessage())))
        .containsInAnyOrder(new PubsubMessage(expected_bytes, Map.of()));
    pipeline.run(OPTIONS);
  }

  private static PubsubDlqSchemaTransform transform(PubsubDlqWriteConfiguration configuration) {
    PubsubDlqSchemaTransformProvider provider = new PubsubDlqSchemaTransformProvider();
    return (PubsubDlqSchemaTransform) provider.from(configuration);
  }
}
