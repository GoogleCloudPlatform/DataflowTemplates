/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.teleport.v2.transforms.ValueExtractorTransform.ValueExtractorFn;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

import java.util.Optional;

/** Contains common transforms for Elasticsearch such as reading and writing. */
public class ElasticsearchTransforms {

  /** Convert provided long to {@link Duration}. */
  private static Duration getDuration(Long milliseconds) {
    return new Duration(milliseconds);
  }

  /**
   * The {@link WriteToElasticsearchOptions} class provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface WriteToElasticsearchOptions extends PipelineOptions {
    @Description(
        "Comma separated list of Elasticsearch nodes to connect to, ex: http://my-node1,http://my-node2")
    @Required
    String getNodeAddresses();

    void setNodeAddresses(String nodeAddresses);

    @Description("The index toward which the requests will be issued, ex: my-index")
    @Required
    String getIndex();

    void setIndex(String index);

    @Description("The document type toward which the requests will be issued, ex: my-document-type")
    @Required
    String getDocumentType();

    void setDocumentType(String documentType);

    @Description("Batch size in number of documents. Default: 1000")
    @Default.Long(1000)
    Long getBatchSize();

    void setBatchSize(Long batchSize);

    @Description("Batch size in number of bytes. Default: 5242880 (5mb)")
    @Default.Long(5242880)
    Long getBatchSizeBytes();

    void setBatchSizeBytes(Long batchSizeBytes);

    @Description("Optional: Max retry attempts, must be > 0, ex: 3. Default: no retries")
    Integer getMaxRetryAttempts();

    void setMaxRetryAttempts(Integer maxRetryAttempts);

    @Description(
        "Optional: Max retry duration in milliseconds, must be > 0, ex: 5000L. Default: no retries")
    Long getMaxRetryDuration();

    void setMaxRetryDuration(Long maxRetryDuration);

    @Description("Set to true to issue partial updates. Default: false")
    @Default.Boolean(false)
    Boolean getUsePartialUpdate();

    void setUsePartialUpdate(Boolean usePartialUpdates);

    @Description(
        "Optional: Path to Javascript function to extract Id from document, ex: gs://path/to/idFn.js. Default: null")
    String getIdFnPath();

    void setIdFnPath(String idFnPath);

    @Description(
        "Optional: Name of Javascript function to extract Id from document, ex: myIdFn. Default: null")
    String getIdFnName();

    void setIdFnName(String idFnName);

    @Description(
        "Optional: Path to Javascript function to extract Index from document that document will be routed to, ex: gs://path/to/indexFn.js. Default: null")
    String getIndexFnPath();

    void setIndexFnPath(String indexFnPath);

    @Description(
        "Optional: Name of Javascript function to extract Index from document, ex: myIndexFn. Default: null")
    String getIndexFnName();

    void setIndexFnName(String indexFnName);

    @Description(
        "Optional: Path to Javascript function to extract Type from document that document will be routed to, ex: gs://path/to/typeFn.js. Default: null")
    String getTypeFnPath();

    void setTypeFnPath(String typeFnPath);

    @Description(
        "Optional: Name of Javascript function to extract Type from document, ex: myTypeFn. Default: null")
    String getTypeFnName();

    void setTypeFnName(String typeFnName);
  }

  /**
   * The {@link WriteToElasticsearch} class writes a {@link PCollection} of strings to Elasticsearch
   * using the following options.
   *
   * <ul>
   *   <li>{@link WriteToElasticsearchOptions#getNodeAddresses()} - comma separated list of nodes.
   *   <li>{@link WriteToElasticsearchOptions#getIndex()} - index to output documents to.
   *   <li>{@link WriteToElasticsearchOptions#getDocumentType()} - document type to write to.
   *   <li>{@link WriteToElasticsearchOptions#getBatchSize()} - batch size in number of documents
   *       (Default:1000).
   *   <li>{@link WriteToElasticsearchOptions#getBatchSizeBytes()} - batch size in number of bytes
   *       (Default:5242880).
   *   <li>{@link WriteToElasticsearchOptions#getMaxRetryAttempts()} - optional: maximum retry
   *       attempts for {@link RetryConfiguration}.
   *   <li>{@link WriteToElasticsearchOptions#getMaxRetryDuration()} - optional: maximum retry
   *       duration for {@link RetryConfiguration}.
   *   <li>{@link WriteToElasticsearchOptions#getUsePartialUpdate()} - use partial updates instead
   *       of insertions (Default: false).
   * </ul>
   *
   * For {@link ElasticsearchIO#write()} with {@link ValueExtractorFn} if the function returns null
   * then the index or type provided as {@link WriteToElasticsearchOptions#getIndex()} or {@link
   * WriteToElasticsearchOptions#getDocumentType()} will be used. For IdFn if function returns null
   * then the id for the document will be assigned by {@link ElasticsearchIO}.
   */
  @AutoValue
  public abstract static class WriteToElasticsearch extends PTransform<PCollection<String>, PDone> {

    public static Builder newBuilder() {
      return new AutoValue_ElasticsearchTransforms_WriteToElasticsearch.Builder();
    }

    public abstract WriteToElasticsearchOptions options();

    @Override
    public PDone expand(PCollection<String> jsonStrings) {

      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              options().getNodeAddresses().split(","),
              options().getIndex(),
              options().getDocumentType());

      ElasticsearchIO.Write write =
          ElasticsearchIO.write()
              .withConnectionConfiguration(config)
              .withMaxBatchSize(options().getBatchSize())
              .withMaxBatchSizeBytes(options().getBatchSizeBytes())
              .withUsePartialUpdate(options().getUsePartialUpdate());

      if (Optional.ofNullable(options().getMaxRetryAttempts()).isPresent()) {
        write.withRetryConfiguration(
            RetryConfiguration.create(
                options().getMaxRetryAttempts(), getDuration(options().getMaxRetryDuration())));
      }

      return jsonStrings.apply(
          "WriteDocuments",
          write
              .withIdFn(
                  ValueExtractorFn.newBuilder()
                      .setFileSystemPath(options().getIdFnPath())
                      .setFunctionName(options().getIdFnName())
                      .build())
              .withIndexFn(
                  ValueExtractorFn.newBuilder()
                      .setFileSystemPath(options().getIndexFnPath())
                      .setFunctionName(options().getIndexFnName())
                      .build())
              .withTypeFn(
                  ValueExtractorFn.newBuilder()
                      .setFileSystemPath(options().getTypeFnPath())
                      .setFunctionName(options().getTypeFnName())
                      .build()));
    }

    /** Builder for {@link WriteToElasticsearch}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOptions(WriteToElasticsearchOptions options);
      
      abstract WriteToElasticsearchOptions options();

      abstract WriteToElasticsearch autoBuild();

      public WriteToElasticsearch build() {

        checkArgument(
            options().getNodeAddresses() != null,
                "Node address(es) must not be null.");

        checkArgument(options().getDocumentType() != null,
                "Document type must not be null.");

        checkArgument(options().getIndex() != null,
                "Index must not be null.");

        checkArgument(
            options().getBatchSize() > 0,
            "Batch size must be > 0. Got: " + options().getBatchSize());

        checkArgument(
            options().getBatchSizeBytes() > 0,
            "Batch size bytes must be > 0. Got: " + options().getBatchSizeBytes());

        /* Check that both {@link RetryConfiguration} parameters are supplied. */
        if (options().getMaxRetryAttempts() != null
            || options().getMaxRetryDuration() != null) {
          checkArgument(
              options().getMaxRetryDuration() != null
                  && options().getMaxRetryAttempts() != null,
              "Both max retry duration and max attempts must be supplied.");
        }

        if (options().getIdFnName() != null || options().getIdFnPath() != null) {
          checkArgument(
              options().getIdFnName() != null && options().getIdFnPath() != null,
              "Both IdFn name and path must be supplied.");
        }

        if (options().getIndexFnName() != null || options().getIndexFnPath() != null) {
          checkArgument(
              options().getIndexFnName() != null && options().getIndexFnPath() != null,
              "Both IndexFn name and path must be supplied.");
        }

        if (options().getTypeFnName() != null || options().getTypeFnPath() != null) {
          checkArgument(
              options().getTypeFnName() != null && options().getTypeFnPath() != null,
              "Both TypeFn name and path must be supplied.");
        }

        return autoBuild();
      }
    }
  }
}
