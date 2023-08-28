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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.StreamingDataGeneratorOptions;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.ReadSpannerSchema;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/** A {@link PTransform} that converts generatedMessages to write to Spanner table. */
@AutoValue
public abstract class StreamingDataGeneratorWriteToSpanner
    extends PTransform<PCollection<byte[]>, PDone> {

  abstract StreamingDataGeneratorOptions getPipelineOptions();

  public static Builder builder(StreamingDataGeneratorOptions options) {
    return new AutoValue_StreamingDataGeneratorWriteToSpanner.Builder().setPipelineOptions(options);
  }

  /** Builder for {@link StreamingDataGeneratorWriteToSpanner}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipelineOptions(StreamingDataGeneratorOptions value);

    public abstract StreamingDataGeneratorWriteToSpanner build();
  }

  @Override
  public PDone expand(PCollection<byte[]> generatedMessages) {
    StreamingDataGeneratorOptions options = getPipelineOptions();
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(options.getProjectId())
            .withInstanceId(options.getSpannerInstanceName())
            .withDatabaseId(options.getSpannerDatabaseName());
    final PCollectionView<Dialect> dialectView =
        // TODO(pranavbhandari): Add support for querying Dialect from Spanner directly
        generatedMessages
            .getPipeline()
            .apply("CreateSingleton", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("As PCollectionView", View.asSingleton());
    final PCollectionView<SpannerSchema> spannerSchemaView =
        generatedMessages
            .getPipeline()
            .apply("Create Seed", Create.of((Void) null))
            .apply(
                "Read information schema",
                ParDo.of(
                        new ReadSpannerSchema(
                            spannerConfig, dialectView, Set.of(options.getSpannerTableName())))
                    .withSideInputs(dialectView))
            .apply("Schema View", View.asSingleton());
    SpannerIO.Write writeIO = SpannerIO.write().withSpannerConfig(spannerConfig);
    if (options.getBatchSizeBytes() != null) {
      writeIO = writeIO.withBatchSizeBytes(options.getBatchSizeBytes());
    }
    if (options.getMaxNumRows() != null) {
      writeIO = writeIO.withMaxNumRows(options.getMaxNumRows());
    }
    if (options.getMaxNumMutations() != null) {
      writeIO = writeIO.withMaxNumMutations(options.getMaxNumMutations());
    }
    if (options.getCommitDeadlineSeconds() != null) {
      writeIO =
          writeIO.withCommitDeadline(Duration.standardSeconds(options.getCommitDeadlineSeconds()));
    }
    generatedMessages
        .apply(
            "Convert to String",
            MapElements.into(TypeDescriptors.strings())
                .via((byte[] element) -> new String(element, StandardCharsets.UTF_8)))
        .apply(
            "Convert to Mutation",
            ParDo.of(new JsonStringToMutationFn(options.getSpannerTableName(), spannerSchemaView))
                .withSideInputs(spannerSchemaView))
        .apply("Write To Spanner", writeIO);
    return PDone.in(generatedMessages.getPipeline());
  }
}
