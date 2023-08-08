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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.ValueExtractorTransform.BooleanValueExtractorFn;
import com.google.cloud.teleport.v2.elasticsearch.transforms.ValueExtractorTransform.StringValueExtractorFn;
import com.google.cloud.teleport.v2.elasticsearch.utils.ConnectionInformation;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIO;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIO.Write.FieldValueExtractFn;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;

/**
 * The {@link WriteToElasticsearch} class writes a {@link PCollection} of strings to Elasticsearch
 * using the following options.
 *
 * <ul>
 *   <li>{@link ElasticsearchWriteOptions#getConnectionUrl()} - CloudId or URL.
 *   <li>{@link ElasticsearchWriteOptions#getIndex()} ()} ()} ()} - Elasticsearch write index.
 *   <li>{@link ElasticsearchWriteOptions#getBatchSize()} - batch size in number of documents
 *       (Default:1000).
 *   <li>{@link ElasticsearchWriteOptions#getBatchSizeBytes()} - batch size in number of bytes
 *       (Default:5242880).
 *   <li>{@link ElasticsearchWriteOptions#getMaxRetryAttempts()} - optional: maximum retry attempts
 *       for {@link ElasticsearchIO.RetryConfiguration}.
 *   <li>{@link ElasticsearchWriteOptions#getMaxRetryDuration()} - optional: maximum retry duration
 *       for {@link ElasticsearchIO.RetryConfiguration}.
 * </ul>
 *
 * For {@link ElasticsearchIO#write()} with {@link ValueExtractorTransform.ValueExtractorFn} if the
 * function returns null then the index or type provided as {@link ConnectionInformation}.
 */
@AutoValue
public abstract class WriteToElasticsearch extends PTransform<PCollection<String>, PDone> {

  /**
   * The {@link StringFieldValueExtractFn} class is a class implementation for {@link
   * FieldValueExtractFn} to extract a property by name.
   */
  @AutoValue
  abstract static class StringFieldValueExtractFn implements FieldValueExtractFn {

    @Nullable
    abstract String propertyName();

    public static Builder newBuilder() {
      return new AutoValue_WriteToElasticsearch_StringFieldValueExtractFn.Builder();
    }

    @Override
    public String apply(JsonNode input) {
      if (propertyName() == null) {
        return null;
      }
      return input.get(propertyName()).asText();
    }

    /** Builder for {@link StringFieldValueExtractFn}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setPropertyName(String propertyName);

      public abstract StringFieldValueExtractFn build();
    }
  }

  /** Convert provided long to {@link Duration}. */
  private static Duration getDuration(Long milliseconds) {
    return new Duration(milliseconds);
  }

  public static Builder newBuilder() {
    return new AutoValue_WriteToElasticsearch.Builder();
  }

  public abstract ElasticsearchWriteOptions options();

  /**
   * Types have been removed in ES 7.0. Default will be _doc. See
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html"
   */
  private static final String DOCUMENT_TYPE = "_doc";

  @Override
  public PDone expand(PCollection<String> jsonStrings) {
    ConnectionInformation connectionInformation =
        new ConnectionInformation(options().getConnectionUrl());

    ElasticsearchIO.ConnectionConfiguration config =
        ElasticsearchIO.ConnectionConfiguration.create(
            new String[] {connectionInformation.getElasticsearchURL().toString()},
            options().getIndex(),
            DOCUMENT_TYPE);

    // If username and password are not blank, use them instead of ApiKey
    if (StringUtils.isNotBlank(options().getElasticsearchUsername())
        && StringUtils.isNotBlank(options().getElasticsearchPassword())) {
      config =
          config
              .withUsername(options().getElasticsearchUsername())
              .withPassword(options().getElasticsearchPassword());
    } else {
      config = config.withApiKey(options().getApiKey());
    }

    if (options().getTrustSelfSignedCerts() != null) {
      config = config.withTrustSelfSignedCerts(options().getTrustSelfSignedCerts());
    }

    ElasticsearchIO.Write elasticsearchWriter =
        ElasticsearchIO.write()
            .withConnectionConfiguration(config)
            .withMaxBatchSize(options().getBatchSize())
            .withMaxBatchSizeBytes(options().getBatchSizeBytes());

    if (options().getPropertyAsId() != null) {
      StringFieldValueExtractFn idFn =
          StringFieldValueExtractFn.newBuilder()
              .setPropertyName(options().getPropertyAsId())
              .build();

      elasticsearchWriter = elasticsearchWriter.withIdFn(idFn);
    } else if (options().getJavaScriptIdFnGcsPath() != null
        && options().getJavaScriptIdFnName() != null) {
      StringValueExtractorFn idFn =
          StringValueExtractorFn.newBuilder()
              .setFileSystemPath(options().getJavaScriptIdFnGcsPath())
              .setFunctionName(options().getJavaScriptIdFnName())
              .build();

      elasticsearchWriter = elasticsearchWriter.withIdFn(idFn);
    }

    if (options().getPropertyAsIndex() != null) {
      StringFieldValueExtractFn indexFn =
          StringFieldValueExtractFn.newBuilder()
              .setPropertyName(options().getPropertyAsIndex())
              .build();

      elasticsearchWriter = elasticsearchWriter.withIndexFn(indexFn);
    } else if (options().getJavaScriptIndexFnGcsPath() != null
        && options().getJavaScriptIndexFnName() != null) {
      StringValueExtractorFn indexFn =
          StringValueExtractorFn.newBuilder()
              .setFileSystemPath(options().getJavaScriptIndexFnGcsPath())
              .setFunctionName(options().getJavaScriptIndexFnName())
              .build();

      elasticsearchWriter = elasticsearchWriter.withIndexFn(indexFn);
    }

    if (options().getJavaScriptTypeFnGcsPath() != null
        && options().getJavaScriptTypeFnName() != null) {
      StringValueExtractorFn typeFn =
          StringValueExtractorFn.newBuilder()
              .setFileSystemPath(options().getJavaScriptTypeFnGcsPath())
              .setFunctionName(options().getJavaScriptTypeFnName())
              .build();

      elasticsearchWriter = elasticsearchWriter.withTypeFn(typeFn);
    }

    if (options().getJavaScriptIsDeleteFnGcsPath() != null
        && options().getJavaScriptIsDeleteFnName() != null) {
      BooleanValueExtractorFn isDeleteFn =
          BooleanValueExtractorFn.newBuilder()
              .setFileSystemPath(options().getJavaScriptIsDeleteFnGcsPath())
              .setFunctionName(options().getJavaScriptIsDeleteFnName())
              .build();

      elasticsearchWriter = elasticsearchWriter.withIsDeleteFn(isDeleteFn);
    }

    if (options().getUsePartialUpdate() != null) {
      elasticsearchWriter =
          elasticsearchWriter.withUsePartialUpdate(
              Boolean.TRUE.equals(options().getUsePartialUpdate()));
    }

    if (options().getBulkInsertMethod() != null) {
      elasticsearchWriter =
          elasticsearchWriter.withBulkInsertMethod(options().getBulkInsertMethod());
    }

    if (Optional.ofNullable(options().getMaxRetryAttempts()).isPresent()) {
      elasticsearchWriter =
          elasticsearchWriter.withRetryConfiguration(
              ElasticsearchIO.RetryConfiguration.create(
                  options().getMaxRetryAttempts(), getDuration(options().getMaxRetryDuration())));
    }

    return jsonStrings.apply("WriteDocuments", elasticsearchWriter);
  }

  /** Builder for {@link WriteToElasticsearch}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOptions(ElasticsearchWriteOptions options);

    abstract ElasticsearchWriteOptions options();

    abstract WriteToElasticsearch autoBuild();

    public WriteToElasticsearch build() {

      checkArgument(options().getConnectionUrl() != null, "ConnectionUrl is required.");

      checkArgument(options().getApiKey() != null, "ApiKey is required.");

      checkArgument(options().getIndex() != null, "Elasticsearch index should not be null.");

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
