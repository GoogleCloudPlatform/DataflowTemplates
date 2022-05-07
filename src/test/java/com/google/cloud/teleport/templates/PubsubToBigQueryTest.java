/*
 * Copyright (C) 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.util.Map;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.PubSubToBigQueryHPI.PubsubMessageToTableRow;
import com.google.cloud.teleport.templates.PubSubToBigQueryHPI.SetBigQueryTableNameFn;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link PubSubToBigQuery} class. */
public class PubsubToBigQueryTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

    private static final String TRANSFORM_FILE_PATH =
            Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

    /** Tests the {@link PubSubToBigQuery} pipeline end-to-end. */
    @Test
    public void testPubsubToBigQueryE2E() throws Exception {
        // Test input
        final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
        final PubsubMessage message = new PubsubMessage(payload.getBytes(), ImmutableMap.of("id",
                "123", "type", "custom_event", "bq_table", "test", "bq_data_set", "abc"));

        final Instant timestamp =
                new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        final FailsafeElementCoder<PubsubMessage, String> coder = FailsafeElementCoder
                .of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        // Parameters
        ValueProvider<String> transformPath = pipeline.newProvider(TRANSFORM_FILE_PATH);
        ValueProvider<String> transformFunction = pipeline.newProvider("transform");

        PubSubToBigQueryHPI.Options options =
                PipelineOptionsFactory.create().as(PubSubToBigQueryHPI.Options.class);

        options.setDatasetNameAttr("bq_data_set");
        options.setOutputDeadletterTable("yin-yang-332008:ope_metrics.%s_error_records");
        options.setJavascriptTextTransformGcsPath(transformPath);
        options.setJavascriptTextTransformFunctionName(transformFunction);

        // Build pipeline
        PCollection<PubsubMessage> messages = pipeline.apply("CreateInput",
                Create.timestamped(TimestampedValue.of(message, timestamp))
                        .withCoder(PubsubMessageWithAttributesCoder.of()));

        PCollectionTuple transformOut =
                messages.apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        // Assert
        PAssert.that(transformOut.get(PubSubToBigQueryHPI.UDF_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(PubSubToBigQueryHPI.TRANSFORM_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(PubSubToBigQueryHPI.TRANSFORM_OUT)).satisfies(collection -> {
            TableRow result = collection.iterator().next();
            assertThat(result.get("ticker"), is(equalTo("GOOGL")));
            assertThat(result.get("price"), is(equalTo(1006.94)));
            return null;
        });

        // Execute pipeline
        pipeline.run();
    }
}
