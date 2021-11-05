/*
 * Copyright (C) 2021 Google LLC
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

import static com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform.readFromKafka;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform;
import com.google.cloud.teleport.v2.options.KafkaToElasticsearchOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.ProcessEventMetadata;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafKaToElasticsearch {

    private static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_OUT =
            new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

    /** The tag for the main output of the json transformation. */
    static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

    /** The tag for the dead-letter output of the udf. */
    static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    static final TupleTag<FailsafeElement<KV<String, String>, String>> TRANSFORM_DEADLETTER_OUT =
            new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

    /** The tag for the error output table of the json to table row transform. */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_ERROROUTPUT_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    /* Logger for class.*/
    private static final Logger LOG = LoggerFactory.getLogger(KafKaToElasticsearch.class);

    public static void main(String[] args) {
        KafkaToElasticsearchOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToElasticsearchOptions.class);

        run(options);
    }

    /**
     * Runs a pipeline which reads message from Kafka and writes to Pub/Sub.
     *
     * @param options arguments to the pipeline
     */
    public static PipelineResult run(KafkaToElasticsearchOptions options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Register the coder for pipeline
        FailsafeElementCoder<KV<String, String>, String> coder =
                FailsafeElementCoder.of(
                        KvCoder.of(
                                NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of())),
                        NullableCoder.of(StringUtf8Coder.of()));

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        List<String> inputTopic = new ArrayList<>(Collections.singleton(options.getInputTopic()));

        String bootstrapServers;
        if (options.getBootstrapServers() != null) {
            bootstrapServers = options.getBootstrapServers();
        } else {
            throw new IllegalArgumentException("Please Provide --bootstrapServers");
        }

        PCollectionTuple convertedKafkaMessages =
            pipeline
                /*
                 * Step #1: Read messages in from Kafka
                 */
                .apply(
                    "ReadFromKafka",
                    readFromKafka(
                        bootstrapServers,
                        inputTopic,
                        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                        null))

                /*
                 * Step #2: Transform the Kafka Messages into Json documents
                 */
                .apply("MapToRecord", ParDo.of(new KafkaTransform.MessageToFailsafeElementFn()))
                    .apply(
                        "InvokeUDF",
                        JavascriptTextTransformer.FailsafeJavascriptUdf.<KV<String, String>>newBuilder()
                            .setSuccessTag(UDF_OUT)
                            .setFailureTag(UDF_DEADLETTER_OUT)
                            .build());

        /*
         * Step #3a: Write Json documents into Elasticsearch using {@link ElasticsearchTransforms.WriteToElasticsearch}.
         */
        convertedKafkaMessages
            .get(UDF_OUT)
            .apply(
                    "GetJsonDocuments",
                    MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
            .apply("Insert metadata", new ProcessEventMetadata())
            .apply(
                    "WriteToElasticsearch",
                    WriteToElasticsearch.newBuilder()
                            .setOptions(options.as(KafkaToElasticsearchOptions.class))
                            .build());

        /*
         * Step 3b: Write elements that failed processing to error output PubSub topic via {@link PubSubIO}.
         */
        /*convertedKafkaMessages
            .get(TRANSFORM_ERROROUTPUT_OUT)
            .apply(ParDo.of(new FailedPubsubMessageToPubsubTopicFn()))
            .apply(
                    "writeFailureMessages",
                    PubsubIO.writeMessages().to(options.getErrorOutputTopic()));*/

        return pipeline.run();
    }

}
