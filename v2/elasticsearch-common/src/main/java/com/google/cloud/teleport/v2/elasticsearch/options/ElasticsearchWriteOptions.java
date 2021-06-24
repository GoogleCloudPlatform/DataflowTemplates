/*
 * Copyright (C) 2021 Google Inc.
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
package com.google.cloud.teleport.v2.elasticsearch.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** The {@link ElasticsearchWriteOptions} with the common write options for Elasticsearch. **/
public interface ElasticsearchWriteOptions extends PipelineOptions {
    @Description(
            "Comma separated list of Elasticsearch nodes to connect to, ex: http://my-node1,http://my-node2")
    @Validation.Required
    String getTargetNodeAddresses();

    void setTargetNodeAddresses(String targetNodeAddresses);

    @Description("The index toward which the requests will be issued, ex: my-index")
    @Validation.Required
    String getWriteIndex();

    void setWriteIndex(String writeIndex);

    @Description("The document type toward which the requests will be issued, ex: my-document-type")
    @Validation.Required
    String getWriteDocumentType();

    void setWriteDocumentType(String writeDocumentType);

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
