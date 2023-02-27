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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class SyndeoPubsubWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SyndeoPubsubWriteSchemaTransformProvider.SyndeoPubsubWriteConfiguration> {

  public static final String VALID_FORMATS_STR = "AVRO,JSON";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  @Override
  public Class<SyndeoPubsubWriteConfiguration> configurationClass() {
    return SyndeoPubsubWriteConfiguration.class;
  }

  @Override
  public SchemaTransform from(SyndeoPubsubWriteConfiguration configuration) {
    if (!VALID_DATA_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
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

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SyndeoPubsubWriteConfiguration {
    @SchemaFieldDescription(
        "The encoding format for the data stored in Pubsub. Valid options are: "
            + VALID_FORMATS_STR)
    public abstract String getFormat();

    @SchemaFieldDescription(
        "The name of the topic to write data into. "
            + "Format: projects/${PROJECT}/topics/${TOPIC}")
    public abstract String getTopic();

    public static SyndeoPubsubWriteConfiguration create(String format, String topic) {
      return new AutoValue_SyndeoPubsubWriteSchemaTransformProvider_SyndeoPubsubWriteConfiguration(
          format, topic);
    }
  }
}
