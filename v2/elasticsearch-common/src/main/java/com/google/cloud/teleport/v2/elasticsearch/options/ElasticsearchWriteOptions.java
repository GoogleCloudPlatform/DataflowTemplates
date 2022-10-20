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
package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.elasticsearch.utils.BulkInsertMethod.BulkInsertMethodOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** The {@link ElasticsearchWriteOptions} with the common write options for Elasticsearch. * */
public interface ElasticsearchWriteOptions extends PipelineOptions {
  @Description("Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId")
  @Validation.Required
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @Description(
      "API key for access without requiring basic authentication. "
          + "Refer  https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request")
  @Validation.Required
  String getApiKey();

  void setApiKey(String apiKey);

  @Description("Username for Elasticsearch endpoint. Overrides ApiKey option if specified.")
  String getElasticsearchUsername();

  void setElasticsearchUsername(String elasticsearchUsername);

  @Description("Password for Elasticsearch endpoint. Overrides ApiKey option if specified.")
  String getElasticsearchPassword();

  void setElasticsearchPassword(String elasticsearchPassword);

  @Description("The index toward which the requests will be issued, ex: my-index")
  String getIndex();

  void setIndex(String index);

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

  @Description(
      "A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precendence over an index UDF)")
  String getPropertyAsIndex();

  void setPropertyAsIndex(String propertyAsIndex);

  @Description(
      "GCS path to JavaScript UDF source for function that will specify _index metadata to be included with document in bulk request")
  String getJavaScriptIndexFnGcsPath();

  void setJavaScriptIndexFnGcsPath(String javaScriptTextTransformGcsPath);

  @Description(
      "UDF JavaScript Function Name for function that will specify _index metadata to be included with document in bulk request")
  String getJavaScriptIndexFnName();

  void setJavaScriptIndexFnName(String javaScriptTextTransformFunctionName);

  @Description(
      "A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precendence over an id UDF)")
  String getPropertyAsId();

  void setPropertyAsId(String propertyAsId);

  @Description(
      "GCS path to JavaScript UDF source for function that will specify _id metadata to be included with document in bulk request")
  String getJavaScriptIdFnGcsPath();

  void setJavaScriptIdFnGcsPath(String javaScriptTextTransformGcsPath);

  @Description(
      "UDF JavaScript Function Name for function that will specify _id metadata to be included with document in bulk request")
  String getJavaScriptIdFnName();

  void setJavaScriptIdFnName(String javaScriptTextTransformFunctionName);

  @Description(
      "GCS path to JavaScript UDF source for function that will specify _type metadata to be included with document in bulk request")
  String getJavaScriptTypeFnGcsPath();

  void setJavaScriptTypeFnGcsPath(String javaScriptTextTransformGcsPath);

  @Description(
      "UDF JavaScript Function Name for function that will specify _type metadata to be included with document in bulk request")
  String getJavaScriptTypeFnName();

  void setJavaScriptTypeFnName(String javaScriptTextTransformFunctionName);

  @Description(
      "GCS path to JavaScript UDF source for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\"")
  String getJavaScriptIsDeleteFnGcsPath();

  void setJavaScriptIsDeleteFnGcsPath(String javaScriptTextTransformGcsPath);

  @Description(
      "UDF JavaScript Function Name for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\"")
  String getJavaScriptIsDeleteFnName();

  void setJavaScriptIsDeleteFnName(String javaScriptTextTransformFunctionName);

  @Description("Whether to use partial updates (update type) with Elasticsearch requests")
  Boolean getUsePartialUpdate();

  void setUsePartialUpdate(Boolean usePartialUpdate);

  @Description(
      "Whether to use INDEX (index, supports upsert) or CREATE (create, errors on duplicate _id) in bulk requests")
  @Default.Enum("CREATE")
  BulkInsertMethodOptions getBulkInsertMethod();

  void setBulkInsertMethod(BulkInsertMethodOptions bulkInsertMethod);
}
