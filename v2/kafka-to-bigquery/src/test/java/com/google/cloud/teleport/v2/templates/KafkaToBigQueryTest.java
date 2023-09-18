/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.KafkaToBigQuery.MessageToTableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link KafkaToBigQuery} class. */
public class KafkaToBigQueryTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

  /** Tests the {@link KafkaToBigQuery} pipeline end-to-end. */
  @Test
  public void testKafkaToBigQueryE2E() {
    // Test input
    final String key = "{\"id\": \"1001\"}";
    final String badKey = "{\"id\": \"1002\"}";
    final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
    final String badPayload = "{\"tickets\": \"AMZ\", \"proctor\": 007";
    final KV<String, String> message = KV.of(key, payload);
    final KV<String, String> badMessage = KV.of(badKey, badPayload);

    final Instant timestamp =
        new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

    final FailsafeElementCoder<KV<String, String>, String> coder =
        FailsafeElementCoder.of(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    KafkaToBigQuery.KafkaToBQOptions options =
        PipelineOptionsFactory.create().as(KafkaToBigQuery.KafkaToBQOptions.class);

    options.setJavascriptTextTransformGcsPath(TRANSFORM_FILE_PATH);
    options.setJavascriptTextTransformFunctionName("transform");

    // Build pipeline
    PCollectionTuple transformOut =
        pipeline
            .apply(
                "CreateInput",
                Create.of(message)
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertMessageToTableRow", new MessageToTableRow(options));

    // Assert
    PAssert.that(transformOut.get(KafkaToBigQuery.UDF_DEADLETTER_OUT)).empty();
    PAssert.that(transformOut.get(KafkaToBigQuery.TRANSFORM_DEADLETTER_OUT)).empty();
    PAssert.that(transformOut.get(KafkaToBigQuery.TRANSFORM_OUT))
        .satisfies(
            collection -> {
              TableRow result = collection.iterator().next();
              assertThat(result.get("ticker"), is(equalTo("GOOGL")));
              assertThat(result.get("price"), is(equalTo(1006.94)));
              return null;
            });

    // Execute pipeline
    pipeline.run();

    // Build pipeline with malformed payload
    PCollectionTuple badTransformOut =
        pipeline
            .apply(
                "CreateBadInput",
                Create.of(badMessage)
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertMessageToTableRow", new MessageToTableRow(options));

    // Assert
    PAssert.that(badTransformOut.get(KafkaToBigQuery.UDF_DEADLETTER_OUT))
        .satisfies(
            collection -> {
              FailsafeElement badResult = collection.iterator().next();
              assertThat(badResult.getOriginalPayload(), is(equalTo(badMessage)));
              assertThat(badResult.getPayload(), is(equalTo(badPayload)));
              return null;
            });
    PAssert.that(badTransformOut.get(KafkaToBigQuery.TRANSFORM_DEADLETTER_OUT)).empty();
    PAssert.that(badTransformOut.get(KafkaToBigQuery.TRANSFORM_OUT)).empty();

    // Execute pipeline
    pipeline.run();
  }
}
