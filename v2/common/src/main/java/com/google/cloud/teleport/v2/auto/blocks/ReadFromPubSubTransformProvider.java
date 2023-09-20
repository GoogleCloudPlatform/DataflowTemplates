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
package com.google.cloud.teleport.v2.auto.blocks;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.auto.value.AutoValue.Builder;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromPubSubTransformProvider.ReadFromPubSubTransformConfiguration;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionTuple;

@AutoService(SchemaTransformProvider.class)
public class ReadFromPubSubTransformProvider
    extends TypedSchemaTransformProvider<ReadFromPubSubTransformConfiguration> {
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract class ReadFromPubSubTransformConfiguration {

    String getInputSubscription();

    public static Builder builder() {
      return new AutoValue_ReadFromPubSubTransformConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setInputSubscription(String subscription);

      public abstract ReadFromPubSubTransformConfiguration build();
    }
  }

  @Override
  public Class<ReadFromPubSubTransformConfiguration> configurationClass() {
    return ReadFromPubSubTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(ReadFromPubSubTransformConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionTuple expand(PCollectionTuple input) {
        return PCollectionTuple.of(
            "output",
            input
                .getPipeline()
                .apply(
                    "ReadPubSubSubscription",
                    PubsubIO.readMessagesWithAttributesAndMessageId()
                        .fromSubscription(configuration.getInputSubscription())));
      }
    };
  }

  @Override
  public String identifier() {
    return "blocks:schematransform:org.apache.beam:read_from_pubsub:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output");
  }
}
