/*
 * Copyright (C) 2019 Google LLC
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

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Takes an input of DataStream events as {@link FailsafeElement} objects and writes them to the
 * given Cloud Spanner database.
 *
 * <p>Each event will be written using a single Cloud Spanner Transaction.
 *
 * <p>The {@link Result} object contains two streams: the successfully written Mutation Group
 * objects with their commit timestamps, and the Mutation Group objects that failed to be written
 * along with the text of the exception that caused the failure.
 */
public class SpannerTransactionWriter
    extends PTransform<
        PCollection<FailsafeElement<String, String>>, SpannerTransactionWriter.Result> {

  /* The spanner config specifying the destination Cloud Spanner database to connect to */
  private final SpannerConfig spannerConfig;

  /* The spanner config specifying the shadow table Cloud Spanner database to connect to */
  private final SpannerConfig shadowTableSpannerConfig;

  /* The information schema of the Cloud Spanner database */
  private final PCollectionView<Ddl> ddlView;

  /* The information schema of the shadow table Cloud Spanner database */
  private final PCollectionView<Ddl> shadowTableDdlView;

  /* The prefix for shadow tables */
  private final String shadowTablePrefix;

  /* The datastream source database type. Eg, MySql or Oracle etc. */
  private final String sourceType;

  /* The run mode, whether it is regular or retry. */
  private final Boolean isRegularRunMode;

  public SpannerTransactionWriter(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.ddlView = ddlView;
    this.shadowTableDdlView = shadowTableDdlView;
    this.shadowTablePrefix = shadowTablePrefix;
    this.sourceType = sourceType;
    this.isRegularRunMode = isRegularRunMode;
  }

  @Override
  public SpannerTransactionWriter.Result expand(
      PCollection<FailsafeElement<String, String>> input) {
    PCollectionTuple spannerWriteResults =
        input.apply(
            "Write Mutations",
            ParDo.of(
                    new SpannerTransactionWriterDoFn(
                        spannerConfig,
                        shadowTableSpannerConfig,
                        ddlView,
                        shadowTableDdlView,
                        shadowTablePrefix,
                        sourceType,
                        isRegularRunMode))
                .withSideInputs(ddlView, shadowTableDdlView)
                .withOutputTags(
                    DatastreamToSpannerConstants.SUCCESSFUL_EVENT_TAG,
                    TupleTagList.of(
                        Arrays.asList(
                            DatastreamToSpannerConstants.PERMANENT_ERROR_TAG,
                            DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG))));

    return Result.create(
        spannerWriteResults.get(DatastreamToSpannerConstants.SUCCESSFUL_EVENT_TAG),
        spannerWriteResults.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG),
        spannerWriteResults.get(DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG));
  }

  /**
   * Container class for the results of this transform.
   *
   * <p>Use {@link #successfulSpannerWrites()}, {@link #permanentErrors()} and {@link
   * #retryableErrors()} to get the three output streams.
   */
  @AutoValue
  public abstract static class Result implements POutput {

    private static Result create(
        PCollection<Timestamp> successfulSpannerWrites,
        PCollection<FailsafeElement<String, String>> permanentErrors,
        PCollection<FailsafeElement<String, String>> retryableErrors) {
      Preconditions.checkNotNull(successfulSpannerWrites);
      Preconditions.checkNotNull(permanentErrors);
      Preconditions.checkNotNull(retryableErrors);
      return new AutoValue_SpannerTransactionWriter_Result(
          successfulSpannerWrites, permanentErrors, retryableErrors);
    }

    public abstract PCollection<Timestamp> successfulSpannerWrites();

    public abstract PCollection<FailsafeElement<String, String>> permanentErrors();

    public abstract PCollection<FailsafeElement<String, String>> retryableErrors();

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      // required by POutput interface.
    }

    @Override
    public Pipeline getPipeline() {
      return successfulSpannerWrites().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          DatastreamToSpannerConstants.SUCCESSFUL_EVENT_TAG,
          successfulSpannerWrites(),
          DatastreamToSpannerConstants.PERMANENT_ERROR_TAG,
          permanentErrors(),
          DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG,
          retryableErrors());
    }
  }
}
