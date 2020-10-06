/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.kafka.consumer.SslConsumerFactoryFn;
import com.google.cloud.teleport.v2.options.KafkaToPubsubOptions;
import com.google.cloud.teleport.v2.templates.KafkaPubsubConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Different transformations over the processed data in the pipeline.
 */
public class FormatTransform {

    /**
     * Configures Kafka consumer.
     *
     * @param bootstrapServers Kafka servers to read from
     * @param topicsList       Kafka topics to read from
     * @param config           configuration for the Kafka consumer
     * @return PCollection of Kafka Key & Value Pair deserialized in string format
     */
    public static PTransform<PBegin, PCollection<KV<String, String>>> readFromKafka(
            String bootstrapServers,
            List<String> topicsList,
            Map<String, Object> config,
            @Nullable Map<String, String> sslConfig
    ) {
        KafkaIO.Read<String, String> kafkaRecords = KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopics(topicsList)
                .withKeyDeserializerAndCoder(
                        StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withValueDeserializerAndCoder(
                        StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withConsumerConfigUpdates(config);
        if (sslConfig != null) {
            kafkaRecords = kafkaRecords.withConsumerFactoryFn(new SslConsumerFactoryFn(sslConfig));
        }
        return kafkaRecords.withoutMetadata();

    }

    /**
     * The {@link MessageToFailsafeElementFn} wraps an Kafka Message with the {@link FailsafeElement}
     * class so errors can be recovered from and the original message can be output to a error records
     * table.
     */
    static class MessageToFailsafeElementFn
            extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<String, String> message = context.element();
            context.output(FailsafeElement.of(message, message.getValue()));
        }
    }

    /**
     * The {@link UdfProcess} enables usage of the User Defined Functions in the pipeline.
     */
    public static class UdfProcess
            extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

        private final KafkaToPubsubOptions options;

        public UdfProcess(KafkaToPubsubOptions options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<KV<String, String>> input) {
            return input
                    // Map the incoming messages into FailsafeElements so we can recover from failures
                    // across multiple transforms.
                    .apply("mapToRecord", ParDo.of(new MessageToFailsafeElementFn()))
                    .apply(
                            "invokeUDF",
                            JavascriptTextTransformer.FailsafeJavascriptUdf.<KV<String, String>>newBuilder()
                                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
                                    .setSuccessTag(KafkaPubsubConstants.UDF_OUT)
                                    .setFailureTag(KafkaPubsubConstants.UDF_DEADLETTER_OUT)
                                    .build());
        }
    }
}
