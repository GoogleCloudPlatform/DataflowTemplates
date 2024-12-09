/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** Takes an input of change stream events and writes them to the source database. */
public class SourceWriterTransform
    extends PTransform<
        PCollection<KV<Long, TrimmedShardedDataChangeRecord>>, SourceWriterTransform.Result> {

  private final Schema schema;
  private final String sourceDbTimezoneOffset;
  private final List<Shard> shards;
  private final SpannerConfig spannerConfig;
  private final Ddl ddl;
  private final String shadowTablePrefix;
  private final String skipDirName;
  private final int maxThreadPerDataflowWorker;
  private final String source;
  private final CustomTransformation customTransformation;

  public SourceWriterTransform(
      List<Shard> shards,
      Schema schema,
      SpannerConfig spannerConfig,
      String sourceDbTimezoneOffset,
      Ddl ddl,
      String shadowTablePrefix,
      String skipDirName,
      int maxThreadPerDataflowWorker,
      String source,
      CustomTransformation customTransformation) {

    this.schema = schema;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.shards = shards;
    this.spannerConfig = spannerConfig;
    this.ddl = ddl;
    this.shadowTablePrefix = shadowTablePrefix;
    this.skipDirName = skipDirName;
    this.maxThreadPerDataflowWorker = maxThreadPerDataflowWorker;
    this.source = source;
    this.customTransformation = customTransformation;
  }

  @Override
  public SourceWriterTransform.Result expand(
      PCollection<KV<Long, TrimmedShardedDataChangeRecord>> input) {
    PCollectionTuple sourceWriteResults =
        input.apply(
            "Write to sourcedb",
            ParDo.of(
                    new SourceWriterFn(
                        this.shards,
                        this.schema,
                        this.spannerConfig,
                        this.sourceDbTimezoneOffset,
                        this.ddl,
                        this.shadowTablePrefix,
                        this.skipDirName,
                        this.maxThreadPerDataflowWorker,
                        this.source,
                        this.customTransformation))
                .withOutputTags(
                    Constants.SUCCESS_TAG,
                    TupleTagList.of(Constants.PERMANENT_ERROR_TAG)
                        .and(Constants.RETRYABLE_ERROR_TAG)
                        .and(Constants.SKIPPED_TAG)
                        .and(Constants.FILTERED_TAG)));

    return Result.create(
        sourceWriteResults.get(Constants.SUCCESS_TAG),
        sourceWriteResults.get(Constants.PERMANENT_ERROR_TAG),
        sourceWriteResults.get(Constants.RETRYABLE_ERROR_TAG),
        sourceWriteResults.get(Constants.SKIPPED_TAG),
        sourceWriteResults.get(Constants.FILTERED_TAG));
  }

  /** Container class for the results of this transform. */
  @AutoValue
  public abstract static class Result implements POutput {

    private static Result create(
        PCollection<String> successfulSourceWrites,
        PCollection<String> permanentErrors,
        PCollection<String> retryableErrors,
        PCollection<String> skippedSourceWrites,
        PCollection<String> filteredWrites) {
      Preconditions.checkNotNull(successfulSourceWrites);
      Preconditions.checkNotNull(permanentErrors);
      Preconditions.checkNotNull(retryableErrors);
      Preconditions.checkNotNull(skippedSourceWrites);
      return new AutoValue_SourceWriterTransform_Result(
          successfulSourceWrites,
          permanentErrors,
          retryableErrors,
          skippedSourceWrites,
          filteredWrites);
    }

    public abstract PCollection<String> successfulSourceWrites();

    public abstract PCollection<String> permanentErrors();

    public abstract PCollection<String> retryableErrors();

    public abstract PCollection<String> skippedSourceWrites();

    public abstract PCollection<String> filteredWrites();

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      // required by POutput interface.
    }

    @Override
    public Pipeline getPipeline() {
      return successfulSourceWrites().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          Constants.SUCCESS_TAG,
          successfulSourceWrites(),
          Constants.PERMANENT_ERROR_TAG,
          permanentErrors(),
          Constants.RETRYABLE_ERROR_TAG,
          retryableErrors(),
          Constants.SKIPPED_TAG,
          skippedSourceWrites(),
          Constants.FILTERED_TAG,
          filteredWrites());
    }
  }
}
