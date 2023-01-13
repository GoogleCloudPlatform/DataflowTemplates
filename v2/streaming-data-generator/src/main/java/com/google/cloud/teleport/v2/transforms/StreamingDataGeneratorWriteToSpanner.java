/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

/** A {@link PTransform} that converts generatedMessages to write to Spanner table. */
@AutoValue
public abstract class StreamingDataGeneratorWriteToSpanner
    extends PTransform<PCollection<byte[]>, PDone> {

  abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

  public static Builder builder(StreamingDataGenerator.StreamingDataGeneratorOptions options) {
    return new AutoValue_StreamingDataGeneratorWriteToSpanner.Builder().setPipelineOptions(options);
  }

  /** Builder for {@link StreamingDataGeneratorWriteToSpanner}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipelineOptions(StreamingDataGenerator.StreamingDataGeneratorOptions value);

    public abstract StreamingDataGeneratorWriteToSpanner build();
  }

  @Override
  public PDone expand(PCollection<byte[]> generatedMessages) {
    StreamingDataGenerator.StreamingDataGeneratorOptions options = getPipelineOptions();
    generatedMessages
        .apply(
            "Convert to String",
            MapElements.into(TypeDescriptors.strings())
                .via((byte[] element) -> new String(element, StandardCharsets.UTF_8)))
        .apply(
            "Convert to Mutation",
            ParDo.of(
                new JsonStringToMutationFn(
                    options.getProjectId(),
                    options.getSpannerInstanceName(),
                    options.getSpannerDatabaseName(),
                    options.getSpannerTableName())))
        .apply(
            "Write To Spanner",
            SpannerIO.write()
                .withInstanceId(options.getSpannerInstanceName())
                .withDatabaseId(options.getSpannerDatabaseName()));
    return PDone.in(generatedMessages.getPipeline());
  }
}
