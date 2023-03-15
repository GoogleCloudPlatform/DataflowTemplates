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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.dlq.pubsub.PubsubDlqSchemaTransformProvider.PubsubDlqWriteConfiguration;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class PubsubDlqSchemaTransformProvider
    extends TypedSchemaTransformProvider<PubsubDlqWriteConfiguration> {

  @Override
  public Class<PubsubDlqWriteConfiguration> configurationClass() {
    return PubsubDlqWriteConfiguration.class;
  }

  @Override
  public SchemaTransform from(PubsubDlqWriteConfiguration configuration) {
    return new PubsubDlqSchemaTransform(configuration);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:pubsub_dlq_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("errors");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.emptyList();
  }
  /**
   * An implementation of {@link SchemaTransform} for Pub/Sub writes configured using {@link
   * PubsubDlqWriteConfiguration}.
   */
  static class PubsubDlqSchemaTransform extends PTransform<PCollectionRowTuple, PCollectionRowTuple>
      implements SchemaTransform {

    public static final int PUBSUB_MESSAGE_SIZE = 10 * 1024 * 1024;
    private final PubsubDlqWriteConfiguration configuration;

    private PubsubClient.PubsubClientFactory pubsubClientFactory;

    PubsubDlqSchemaTransform(PubsubDlqWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    PubsubDlqSchemaTransform withPubsubClientFactory(PubsubClient.PubsubClientFactory factory) {
      this.pubsubClientFactory = factory;
      return this;
    }

    /** Implements {@link SchemaTransform} buildTransform method. */
    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return this;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<PubsubMessage> messages =
          input.get("errors").apply(ParDo.of(new ConvertRowToMessage()));

      PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(configuration.getTopic());
      if (pubsubClientFactory != null) {
        write = write.withClientFactory(pubsubClientFactory);
      }
      messages.apply(PubsubIO.Write.class.getSimpleName(), write);

      return PCollectionRowTuple.empty(input.getPipeline());
    }

    public static class ConvertRowToMessage extends DoFn<Row, PubsubMessage> {
      @ProcessElement
      public void process(@Element Row row, OutputReceiver<PubsubMessage> receiver) {
        byte[] message = row.toString(true).getBytes(StandardCharsets.UTF_8);
        if (message.length < PUBSUB_MESSAGE_SIZE) {
          receiver.output(new PubsubMessage(message, Map.of()));
        } else {
          byte[] truncated = new byte[PUBSUB_MESSAGE_SIZE];
          System.arraycopy(message, 0, truncated, 0, PUBSUB_MESSAGE_SIZE);
          receiver.output(new PubsubMessage(truncated, Map.of()));
        }
      }
    }
  }

  @AutoValue
  public abstract static class PubsubDlqWriteConfiguration {
    public abstract String getTopic();

    public static PubsubDlqWriteConfiguration create(String topic) {
      return new AutoValue_PubsubDlqSchemaTransformProvider_PubsubDlqWriteConfiguration(topic);
    }
  }
}
