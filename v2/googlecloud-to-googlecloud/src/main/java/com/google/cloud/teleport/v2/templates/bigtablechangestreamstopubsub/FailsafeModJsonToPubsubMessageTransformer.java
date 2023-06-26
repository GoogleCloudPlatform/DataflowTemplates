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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.v2.DataChangeRecord;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils.PubSubUtils;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.Encoding;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.Encoder;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import java.io.ByteArrayOutputStream;
import com.google.cloud.teleport.v2.ChangeLogEntryProto.ChangelogEntryProto;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;

/**
 * Class {@link FailsafeModJsonToPubsubMessageTransformer} provides methods that convert a
 * {@link Mod} JSON string wrapped in {@link FailsafeElement} to a {@link PubsubMessage}.
 */
public final class FailsafeModJsonToPubsubMessageTransformer {

    /**
     * Primary class for taking a {@link FailsafeElement} {@link Mod} JSON input and converting to a
     * {@link PubsubMessage}.
     */
    public static class FailsafeModJsonToPubsubMessage
            extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

        private final PubSubUtils pubSubUtils;
        private static final Logger LOG = LoggerFactory.getLogger(FailsafeModJsonToPubsubMessageTransformer.class);

        private static final String NATIVE_CLIENT = "native_client";
        private static final String PUBSUBIO = "pubsubio";


        /**
         * The tag for the main output of the transformation.
         */
        public TupleTag<com.google.pubsub.v1.PubsubMessage> transformOut = new TupleTag<>() {
        };

        /**
         * The tag for the dead letter output of the transformation.
         */
        public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut =
                new TupleTag<>() {
                };

        private final FailsafeModJsonToPubsubMessageOptions failsafeModJsonToPubsubMessageOptions;

        public FailsafeModJsonToPubsubMessage(
                PubSubUtils pubSubUtils,
                FailsafeModJsonToPubsubMessageOptions failsafeModJsonToPubsubMessageOptions) {
            this.pubSubUtils = pubSubUtils;
            this.failsafeModJsonToPubsubMessageOptions = failsafeModJsonToPubsubMessageOptions;
        }

        public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {
            PCollectionTuple out =
                    input.apply(
                            ParDo.of(
                                            new FailsafeModJsonToPubsubMessageFn(
                                                    pubSubUtils,
                                                    transformOut,
                                                    transformDeadLetterOut))
                                    .withOutputTags(transformOut, TupleTagList.of(transformDeadLetterOut)));
            out.get(transformDeadLetterOut).setCoder(failsafeModJsonToPubsubMessageOptions.getCoder());
            return out;
        }

        /**
         * The {@link FailsafeModJsonToPubsubMessageFn} converts a {@link Mod} JSON string wrapped in {@link
         * FailsafeElement} to a {@link PubsubMesage}.
         */
        public static class FailsafeModJsonToPubsubMessageFn
                extends DoFn<FailsafeElement<String, String>, PubsubMessage> {
            private final PubSubUtils pubSubUtils;
            public TupleTag<PubsubMessage> transformOut;
            public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut;
            private transient Publisher publisher;

            public FailsafeModJsonToPubsubMessageFn(
                    PubSubUtils pubSubUtils,
                    TupleTag<PubsubMessage> transformOut,
                    TupleTag<FailsafeElement<String, String>> transformDeadLetterOut) {
                this.pubSubUtils = pubSubUtils;
                this.transformOut = transformOut;
                this.transformDeadLetterOut = transformDeadLetterOut;
            }

            @Setup
            public void setUp() {
                try {
                    final TopicName projectTopicName = TopicName.of(
                            pubSubUtils.getDestination().getPubSubProject(), pubSubUtils.getDestination().getPubSubTopic());
                    publisher = Publisher.newBuilder(projectTopicName).build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Teardown
            public void tearDown() {
                try {
                    if (publisher != null) {
                        publisher.shutdown();
                        publisher.awaitTermination(5, TimeUnit.MINUTES);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private static ChangelogEntry mapJsonToChangelogEntry(String jsonString) throws Exception {
                // Assuming you have the Avro schema generated as MyClass.getClassSchema()
                Schema schema = ChangelogEntry.getClassSchema();

                // Create a Decoder for JSON data
                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);

                DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

                // Read the GenericRecord from the JSON data
                GenericRecord record = reader.read(null, decoder);
                ChangelogEntry changelogEntry = ChangelogEntry.newBuilder().build();

                // set fields and map part
                return changelogEntry;
            }

            private static ChangelogEntryProto mapJsonToChangelogEntryProto(String jsonString) throws Exception {
                ChangelogEntryProto.Builder builder = ChangelogEntryProto.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builder);

                // Build the protobuf message
                ChangelogEntryProto changelogEntryProto = builder.build();
                return changelogEntryProto;
            }

            @ProcessElement
            public void processElement(ProcessContext context) {
                LOG.info("Reading a failsafeModJsonString");
                FailsafeElement<String, String> failsafeModJsonString = context.element();

                LOG.info("");

                try {
                    com.google.pubsub.v1.PubsubMessage pubSubMessage = publishModJsonStringToPubsubMessage(failsafeModJsonString.getPayload());

                    context.output(pubSubMessage);
                } catch (Exception e) {
                    LOG.error("Writing to transform dead letter out", e);
                    context.output(
                            transformDeadLetterOut,
                            FailsafeElement.of(failsafeModJsonString)
                                    .setErrorMessage(e.getMessage())
                                    .setStacktrace(Throwables.getStackTraceAsString(e)));
                }
            }
            /* Schema Details:  */
            private PubsubMessage publishModJsonStringToPubsubMessage(String modJsonString)
                    throws Exception {
                String messageFormat = pubSubUtils.getDestination().getMessageFormat();
                String messageEncoding = pubSubUtils.getDestination().getMessageEncoding();
                Publisher publisher = null;
                Encoding encoding = null;

                block:
                try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
                    GetTopicRequest request =
                            GetTopicRequest.newBuilder()
                                    .setTopic(TopicName.ofProjectTopicName(
                                            pubSubUtils.getDestination().getPubSubProject(),
                                            pubSubUtils.getDestination().getPubSubTopic()).toString())
                                    .build();
                    Topic topic = topicAdminClient.getTopic(request);
                    encoding = topic.getSchemaSettings().getEncoding();
                    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                    if (!topic.getSchemaSettings().getSchema().isEmpty()){
                        messageFormat = topic.getSchemaSettings().getSchema();
                        messageEncoding = topic.getSchemaSettings().getEncoding().toString();
                    }
                    switch(messageFormat){
                        case "AVRO":
                            ChangelogEntry changelogEntry = mapJsonToChangelogEntry(modJsonString);
                            Encoder encoder = null;
                            switch(messageEncoding) {
                                case "BINARY":
                                    System.out.println("Preparing a BINARY encoder...");
                                    encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /*reuse=*/ null);
                                    break;
                                case "JSON":
                                    System.out.println("Preparing a JSON encoder...");
                                    encoder = EncoderFactory.get().jsonEncoder(ChangelogEntry.getClassSchema(), byteStream);
                                    break;
                                default:
                                    break block;
                            }
                            changelogEntry.customEncode(encoder);
                            encoder.flush();

                            // Publish the encoded object as a Pub/Sub message.
                            ByteString data = ByteString.copyFrom(byteStream.toByteArray());
                            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
                            System.out.println("Publishing message: " + message);

                            ApiFuture<String> future = publisher.publish(message);
                            System.out.println("Published message ID: " + future.get());


                        case "Protocol Buffer":
                            ChangelogEntryProto changelogEntryProto = mapJsonToChangelogEntryProto(modJsonString);
                            data = ByteString.copyFrom(byteStream.toByteArray());
                            PubsubMessage.Builder protoMessage = PubsubMessage.newBuilder();
                            switch(messageEncoding) {
                                case "BINARY":
                                    protoMessage.setData(changelogEntryProto.toByteString());
                                    System.out.println("Publishing a BINARY-formatted message:\n" + protoMessage);
                                    break;

                                case "JSON":
                                    String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(changelogEntryProto);
                                    protoMessage.setData(ByteString.copyFromUtf8(jsonString));
                                    System.out.println("Publishing a JSON-formatted message:\n" + protoMessage);
                                default:
                                    break block;
                            }
                            future = publisher.publish(protoMessage.build());
                            System.out.println("Published message ID: " + future.get());

                        case "JSON":
                            byte[] encodedRecords = modJsonString.getBytes();

                            com.google.pubsub.v1.PubsubMessage v1PubsubMessage =
                                    com.google.pubsub.v1.PubsubMessage.newBuilder()
                                            .setData(ByteString.copyFrom(encodedRecords))
                                            .build();
                            ApiFuture<String> messageIdFuture = publisher.publish(v1PubsubMessage);
                            List<ApiFuture<String>> futures = new ArrayList();
                            futures.add(messageIdFuture);
                            ApiFutures.allAsList(futures).get();
                            return v1PubsubMessage;
                        default:
                            final String errorMessage =
                                    "Invalid output format:"
                                            + messageFormat
                                            + ". Supported output formats: JSON, AVRO";
                            LOG.info(errorMessage);
                            throw new IllegalArgumentException(errorMessage);
                    }
                } catch (Exception e) {
                    throw e;
                }

                byte[] encodedRecords = modJsonString.getBytes();

                com.google.pubsub.v1.PubsubMessage v1PubsubMessage =
                        com.google.pubsub.v1.PubsubMessage.newBuilder()
                                .setData(ByteString.copyFrom(encodedRecords))
                                .build();
                ApiFuture<String> messageIdFuture = publisher.publish(v1PubsubMessage);
                List<ApiFuture<String>> futures = new ArrayList();
                futures.add(messageIdFuture);
                ApiFutures.allAsList(futures).get();
                return v1PubsubMessage;
            }
        }
    }
    /**
     * {@link FailsafeModJsonToPubsubMessageOptions} provides options to initialize {@link
     * FailsafeModJsonToPubsubMessageTransformer}.
     */
    @AutoValue
    public abstract static class FailsafeModJsonToPubsubMessageOptions implements Serializable {

        public abstract FailsafeElementCoder<String, String> getCoder();

        static Builder builder() {
            return new AutoValue_FailsafeModJsonToPubsubMessageTransformer_FailsafeModJsonToPubsubMessageOptions
                    .Builder();
        }

        @AutoValue.Builder
        abstract static class Builder {

            abstract Builder setCoder(FailsafeElementCoder<String, String> coder);

            abstract FailsafeModJsonToPubsubMessageOptions build();
        }
    }
}