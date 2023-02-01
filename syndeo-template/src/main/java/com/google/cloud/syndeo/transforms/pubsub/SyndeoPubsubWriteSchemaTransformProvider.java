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
package com.google.cloud.syndeo.transforms.pubsub;

import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class SyndeoPubsubWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SyndeoPubsubWriteSchemaTransformProvider.SyndeoPubsubWriteConfiguration> {

  @Override
  public Class<SyndeoPubsubWriteConfiguration> configurationClass() {
    return SyndeoPubsubWriteConfiguration.class;
  }

  @Override
  public SchemaTransform from(SyndeoPubsubWriteConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            SerializableFunction<Row, byte[]> fn =
                configuration.getFormat().equals("AVRO")
                    ? AvroUtils.getRowToAvroBytesFunction(input.get("input").getSchema())
                    : JsonUtils.getRowToJsonBytesFunction(input.get("input").getSchema());
            input
                .get("input")
                .apply(
                    MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                        .via(
                            row -> {
                              return new PubsubMessage(fn.apply(row), Map.of());
                            }))
                .apply(PubsubIO.writeMessages().to(configuration.getTopic()));
            return PCollectionRowTuple.empty(input.getPipeline());
          }
        };
      }
    };
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:pubsub_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.emptyList();
  }

  @AutoValue
  public abstract static class SyndeoPubsubWriteConfiguration {
    public abstract String getFormat();

    public abstract String getTopic();

    public static SyndeoPubsubWriteConfiguration create(String format, String topic) {
      return new AutoValue_SyndeoPubsubWriteSchemaTransformProvider_SyndeoPubsubWriteConfiguration(
          format, topic);
    }
  }
}
