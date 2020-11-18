/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package de.tillhub.transactions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.PubSubToBigQuery.PubsubMessageToTableRow;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import de.tillhub.templates.RealtimeTransactionsETLFlatTable;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

/** Test cases for the {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable} class. */
public class TransactionsTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "udf/";

    private static final String TRANSFORM_FILE_PATH =
            Resources.getResource(RESOURCES_DIR + "indexFlatTable.js").getPath();


    /** Tests the {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable} pipeline end-to-end. */
    @Test
    public void testSimpleTransaction() throws Exception {
        // Test input
        final URL url = Resources.getResource(RESOURCES_DIR + "/examples/offendingTx.json");
        final String payload = Resources.toString(url, StandardCharsets.UTF_8);
       // final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
        final PubsubMessage message =
                new PubsubMessage(payload.getBytes(), ImmutableMap.of("client_account", "123", "transaction", "567"));

        final Instant timestamp =
                new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

        final FailsafeElementCoder<PubsubMessage, String> coder =
                FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        // Parameters
        ValueProvider<String> transformPath = pipeline.newProvider(TRANSFORM_FILE_PATH);
        ValueProvider<String> transformFunction = pipeline.newProvider("transformFlatTableTransaction");

        RealtimeTransactionsETLFlatTable.Options options =
                PipelineOptionsFactory.create().as(RealtimeTransactionsETLFlatTable.Options.class);

        options.setJavascriptTextTransformGcsPath(transformPath);
        options.setJavascriptTextTransformFunctionName(transformFunction);
        RealtimeTransactionsETLFlatTable.initResultHolders();
        // Build pipeline
        PCollectionTuple transformOut =
                pipeline
                        .apply(
                                "CreateInput",
                                Create.timestamped(TimestampedValue.of(message, timestamp))
                                        .withCoder(PubsubMessageWithAttributesCoder.of()))
                        .apply("ConvertMessageToTableRow", new de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageArrayToTableRow(options, "transformFlatTableTransaction"));
        // Assert
        PAssert.that(transformOut.get(RealtimeTransactionsETLFlatTable.UDF_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(RealtimeTransactionsETLFlatTable.TRANSFORM_DEADLETTER_OUT)).empty();
        PAssert.that(transformOut.get(RealtimeTransactionsETLFlatTable.TRANSFORM_OUT))
                .satisfies(
                        collection -> {
                            TableRow result = collection.iterator().next();
                            assertThat(result.get("oltp_entity_id"), is(equalTo("567")));
                            assertThat(result.get("guid"), is(equalTo("9118236a-d652-49c8-8e3c-9568bf26189e")));
                            return null;
                        });

        // Execute pipeline
        pipeline.run();
    }
}
