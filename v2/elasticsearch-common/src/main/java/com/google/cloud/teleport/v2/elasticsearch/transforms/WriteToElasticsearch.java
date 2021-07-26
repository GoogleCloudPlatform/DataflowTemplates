/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import java.util.Optional;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

/**
 * The {@link WriteToElasticsearch} class writes a {@link PCollection} of strings to Elasticsearch
 * using the following options.
 *
 * <ul>
 *   <li>{@link ElasticsearchWriteOptions#getTargetNodeAddresses()} - comma separated list of nodes.
 *   <li>{@link ElasticsearchWriteOptions#getWriteIndex()} - index to output documents to.
 *   <li>{@link ElasticsearchWriteOptions#getWriteDocumentType()} - document type to write to.
 *   <li>{@link ElasticsearchWriteOptions#getBatchSize()} - batch size in number of documents
 *       (Default:1000).
 *   <li>{@link ElasticsearchWriteOptions#getBatchSizeBytes()} - batch size in number of bytes
 *       (Default:5242880).
 *   <li>{@link ElasticsearchWriteOptions#getMaxRetryAttempts()} - optional: maximum retry attempts
 *       for {@link ElasticsearchIO.RetryConfiguration}.
 *   <li>{@link ElasticsearchWriteOptions#getMaxRetryDuration()} - optional: maximum retry duration
 *       for {@link ElasticsearchIO.RetryConfiguration}.
 *   <li>{@link ElasticsearchWriteOptions#getUsePartialUpdate()} - use partial updates instead of
 *       insertions (Default: false).
 * </ul>
 *
 * For {@link ElasticsearchIO#write()} with {@link ValueExtractorTransform.ValueExtractorFn} if the
 * function returns null then the index or type provided as {@link
 * ElasticsearchWriteOptions#getWriteIndex()} or {@link
 * ElasticsearchWriteOptions#getWriteDocumentType()} will be used. For IdFn if function returns null
 * then the id for the document will be assigned by {@link ElasticsearchIO}.
 */
@AutoValue
public abstract class WriteToElasticsearch extends PTransform<PCollection<String>, PDone> {

  /** Convert provided long to {@link Duration}. */
  private static Duration getDuration(Long milliseconds) {
    return new Duration(milliseconds);
  }

  public static Builder newBuilder() {
    return new AutoValue_WriteToElasticsearch.Builder();
  }

  public abstract ElasticsearchWriteOptions options();

  @Override
  public PDone expand(PCollection<String> jsonStrings) {

    ElasticsearchIO.ConnectionConfiguration config =
        ElasticsearchIO.ConnectionConfiguration.create(
            options().getTargetNodeAddresses().split(","),
            options().getWriteIndex(),
            options().getWriteDocumentType());

    ElasticsearchIO.Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(config)
            .withMaxBatchSize(options().getBatchSize())
            .withMaxBatchSizeBytes(options().getBatchSizeBytes())
            .withUsePartialUpdate(options().getUsePartialUpdate());

    if (Optional.ofNullable(options().getMaxRetryAttempts()).isPresent()) {
      write.withRetryConfiguration(
          ElasticsearchIO.RetryConfiguration.create(
              options().getMaxRetryAttempts(), getDuration(options().getMaxRetryDuration())));
    }

    return jsonStrings.apply("WriteDocuments", write);
  }

  /** Builder for {@link WriteToElasticsearch}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOptions(ElasticsearchWriteOptions options);

    abstract ElasticsearchWriteOptions options();

    abstract WriteToElasticsearch autoBuild();

    public WriteToElasticsearch build() {

      checkArgument(
          options().getTargetNodeAddresses() != null, "Target Node address(es) must not be null.");

      checkArgument(
          options().getWriteDocumentType() != null, "Write Document type must not be null.");

      checkArgument(options().getWriteIndex() != null, "Write Index must not be null.");

      checkArgument(
          options().getBatchSize() > 0, "Batch size must be > 0. Got: " + options().getBatchSize());

      checkArgument(
          options().getBatchSizeBytes() > 0,
          "Batch size bytes must be > 0. Got: " + options().getBatchSizeBytes());

      /* Check that both {@link RetryConfiguration} parameters are supplied. */
      if (options().getMaxRetryAttempts() != null || options().getMaxRetryDuration() != null) {
        checkArgument(
            options().getMaxRetryDuration() != null && options().getMaxRetryAttempts() != null,
            "Both max retry duration and max attempts must be supplied.");
      }

      return autoBuild();
    }
  }
}
