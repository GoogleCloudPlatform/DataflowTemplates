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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.elasticsearch.utils.BulkInsertMethod.BulkInsertMethodOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** The {@link ElasticsearchWriteOptions} with the common write options for Elasticsearch. * */
public interface ElasticsearchWriteOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      description = "Elasticsearch URL or CloudID if using Elastic Cloud",
      helpText =
          "Elasticsearch URL in the format https://hostname:[port] or specify CloudID if using Elastic Cloud",
      example = "https://elasticsearch-host:9200")
  @Validation.Required
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @TemplateParameter.Text(
      order = 2,
      description = "Base64 Encoded API Key for access without requiring basic authentication",
      helpText =
          "Base64 Encoded API Key for access without requiring basic authentication. Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request")
  @Validation.Required
  String getApiKey();

  void setApiKey(String apiKey);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "Username for Elasticsearch endpoint",
      helpText = "Username for Elasticsearch endpoint. Overrides ApiKey option if specified")
  String getElasticsearchUsername();

  void setElasticsearchUsername(String elasticsearchUsername);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description = "Password for Elasticsearch endpoint",
      helpText = "Password for Elasticsearch endpoint. Overrides ApiKey option if specified")
  String getElasticsearchPassword();

  void setElasticsearchPassword(String elasticsearchPassword);

  @TemplateParameter.Text(
      order = 5,
      optional = false,
      regexes = {"[a-zA-Z0-9._-]+"},
      description = "Elasticsearch index",
      helpText = "The index toward which the requests will be issued",
      example = "my-index")
  String getIndex();

  void setIndex(String index);

  @TemplateParameter.Long(
      order = 6,
      optional = true,
      description = "Batch Size",
      helpText = "Batch Size used for batch insertion of messages into Elasticsearch.")
  @Default.Long(1000)
  Long getBatchSize();

  void setBatchSize(Long batchSize);

  @TemplateParameter.Long(
      order = 7,
      optional = true,
      description = "Batch Size in Bytes",
      helpText =
          "Batch Size in bytes used for batch insertion of messages into elasticsearch. Default: 5242880 (5mb)")
  @Default.Long(5242880)
  Long getBatchSizeBytes();

  void setBatchSizeBytes(Long batchSizeBytes);

  @TemplateParameter.Integer(
      order = 8,
      optional = true,
      description = "Max retry attempts.",
      helpText = "Max retry attempts, must be > 0. Default: no retries")
  Integer getMaxRetryAttempts();

  void setMaxRetryAttempts(Integer maxRetryAttempts);

  @TemplateParameter.Long(
      order = 9,
      optional = true,
      description = "Max retry duration.",
      helpText = "Max retry duration in milliseconds, must be > 0. Default: no retries")
  Long getMaxRetryDuration();

  void setMaxRetryDuration(Long maxRetryDuration);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Document property to specify _index metadata",
      helpText =
          "A property in the document being indexed whose value will specify _index metadata to be included with document in bulk request (takes precedence over an _index UDF).")
  String getPropertyAsIndex();

  void setPropertyAsIndex(String propertyAsIndex);

  @TemplateParameter.GcsReadFile(
      order = 11,
      optional = true,
      description = "Cloud Storage path to JavaScript UDF source for _index metadata",
      helpText =
          "Cloud Storage path to JavaScript UDF source for function that will specify _index metadata to be included with document in bulk request.")
  String getJavaScriptIndexFnGcsPath();

  void setJavaScriptIndexFnGcsPath(String javaScriptTextTransformGcsPath);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "UDF JavaScript Function Name for _index metadata",
      helpText =
          "UDF JavaScript Function Name for function that will specify _index metadata to be included with document in bulk request")
  String getJavaScriptIndexFnName();

  void setJavaScriptIndexFnName(String javaScriptTextTransformFunctionName);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "Document property to specify _id metadata",
      helpText =
          "A property in the document being indexed whose value will specify _id metadata to be included with document in bulk request (takes precedence over an _id UDF).")
  String getPropertyAsId();

  void setPropertyAsId(String propertyAsId);

  @TemplateParameter.GcsReadFile(
      order = 14,
      optional = true,
      description = "Cloud Storage path to JavaScript UDF source for _id metadata",
      helpText =
          "Cloud Storage path to JavaScript UDF source for function that will specify _id metadata to be included with document in bulk request.")
  String getJavaScriptIdFnGcsPath();

  void setJavaScriptIdFnGcsPath(String javaScriptTextTransformGcsPath);

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      description = "UDF JavaScript Function Name for _id metadata",
      helpText =
          "UDF JavaScript Function Name for function that will specify _id metadata to be included with document in bulk request.")
  String getJavaScriptIdFnName();

  void setJavaScriptIdFnName(String javaScriptTextTransformFunctionName);

  @TemplateParameter.GcsReadFile(
      order = 16,
      optional = true,
      description = "Cloud Storage path to JavaScript UDF source for _type metadata",
      helpText =
          "Cloud Storage path to JavaScript UDF source for function that will specify _type metadata to be included with document in bulk request.")
  String getJavaScriptTypeFnGcsPath();

  void setJavaScriptTypeFnGcsPath(String javaScriptTextTransformGcsPath);

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "UDF JavaScript Function Name for _type metadata",
      helpText =
          "UDF JavaScript Function Name for function that will specify _type metadata to be included with document in bulk request")
  String getJavaScriptTypeFnName();

  void setJavaScriptTypeFnName(String javaScriptTextTransformFunctionName);

  @TemplateParameter.GcsReadFile(
      order = 18,
      optional = true,
      description = "Cloud Storage path to JavaScript UDF source for isDelete function",
      helpText =
          "Cloud Storage path to JavaScript UDF source for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\".")
  String getJavaScriptIsDeleteFnGcsPath();

  void setJavaScriptIsDeleteFnGcsPath(String javaScriptTextTransformGcsPath);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      description = "UDF JavaScript Function Name for isDelete",
      helpText =
          "UDF JavaScript Function Name for function that will determine if document should be deleted rather than inserted or updated, function should return string value \"true\" or \"false\".")
  String getJavaScriptIsDeleteFnName();

  void setJavaScriptIsDeleteFnName(String javaScriptTextTransformFunctionName);

  @TemplateParameter.Boolean(
      order = 20,
      optional = true,
      description = "Use partial updates",
      helpText =
          "Whether to use partial updates (update rather than create or index, allowing partial docs) with Elasticsearch requests.")
  @Default.Boolean(false)
  Boolean getUsePartialUpdate();

  void setUsePartialUpdate(Boolean usePartialUpdate);

  @TemplateParameter.Enum(
      order = 21,
      enumOptions = {"INDEX", "CREATE"},
      optional = true,
      description = "Build insert method",
      helpText =
          "Whether to use INDEX (index, allows upsert) or CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests.")
  @Default.Enum("CREATE")
  BulkInsertMethodOptions getBulkInsertMethod();

  void setBulkInsertMethod(BulkInsertMethodOptions bulkInsertMethod);

  @TemplateParameter.Boolean(
      order = 22,
      optional = true,
      description = "trust self-signed certificate",
      helpText =
          "Whether to trust self-signed certificate or not. An elasticsearch instance installed might have a self-signed certificate, Enable this to True to by-pass the validation on SSL certificate. (default is False)")
  @Default.Boolean(false)
  Boolean getTrustSelfSignedCerts();

  void setTrustSelfSignedCerts(Boolean trustSelfSignedCerts);
}
