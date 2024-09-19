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
package com.google.cloud.teleport.v2.mongodb.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link MongoDbToBigQueryOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public class MongoDbToBigQueryOptions {

  /** Options for Reading MongoDb Documents. */
  public interface MongoDbOptions extends PipelineOptions, DataflowPipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        description = "MongoDB Connection URI",
        helpText = "The MongoDB connection URI in the format `mongodb+srv://:@.`")
    String getMongoDbUri();

    void setMongoDbUri(String getMongoDbUri);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        description = "MongoDB database",
        helpText = "Database in MongoDB to read the collection from.",
        example = "my-db")
    String getDatabase();

    void setDatabase(String database);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        description = "MongoDB collection",
        helpText = "Name of the collection inside MongoDB database.",
        example = "my-collection")
    String getCollection();

    void setCollection(String collection);

    @TemplateParameter.Enum(
        order = 4,
        enumOptions = {@TemplateEnumOption("FLATTEN"), @TemplateEnumOption("NONE")},
        description = "User option",
        helpText =
            "`FLATTEN`, `JSON`, or `NONE`. `FLATTEN` flattens the documents to the single level. `JSON` stores document in BigQuery JSON format. `NONE` stores the whole document as a JSON-formatted STRING.")
    @Default.String("NONE")
    String getUserOption();

    void setUserOption(String userOption);

    @TemplateParameter.KmsEncryptionKey(
        order = 5,
        optional = true,
        description = "Google Cloud KMS key",
        helpText =
            "Cloud KMS Encryption Key to decrypt the mongodb uri connection string. If Cloud KMS key is "
                + "passed in, the mongodb uri connection string must all be passed in encrypted.",
        example =
            "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
    String getKMSEncryptionKey();

    void setKMSEncryptionKey(String keyName);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Source",
        description = "Bson filter",
        optional = true,
        helpText = "Bson filter in json format.",
        example = "{ \"val\": { $gt: 0, $lt: 9 }}")
    String getFilter();

    void setFilter(String jsonFilter);
  }

  /** Options for reading from PubSub. */
  public interface PubSubOptions extends PipelineOptions, DataflowPipelineOptions {
    @TemplateParameter.PubsubTopic(
        order = 1,
        groupName = "Source",
        description = "Pub/Sub input topic",
        helpText =
            "The Pub/Sub input topic to read from, in the format of `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.")
    String getInputTopic();

    void setInputTopic(String inputTopic);
  }

  /** Options for Reading BigQuery Rows. */
  public interface BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        groupName = "Target",
        description = "BigQuery output table",
        helpText =
            "The BigQuery table to write to. For example, `bigquery-project:dataset.output_table`.")
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);

    @TemplateParameter.GcsReadFile(
        order = 2,
        optional = true,
        description = "Cloud Storage path to BigQuery JSON schema",
        helpText = "The Cloud Storage path for the BigQuery JSON schema.",
        example = "gs://your-bucket/your-schema.json")
    String getBigQuerySchemaPath();

    void setBigQuerySchemaPath(String path);
  }

  /** UDF options. */
  public interface JavascriptDocumentTransformerOptions extends PipelineOptions {
    @TemplateParameter.JavascriptUdfFile(
        order = 1,
        optional = true,
        description = "JavaScript UDF path in Cloud Storage.",
        helpText =
            "The Cloud Storage URI of the `.js` file that defines the JavaScript user-defined function (UDF) to use.",
        example = "gs://your-bucket/your-transforms/*.js")
    String getJavascriptDocumentTransformGcsPath();

    void setJavascriptDocumentTransformGcsPath(String javascriptDocumentTransformGcsPath);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "The name of the JavaScript function to call as your UDF.",
        helpText =
            "The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is myTransform. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples).",
        example = "transform")
    String getJavascriptDocumentTransformFunctionName();

    void setJavascriptDocumentTransformFunctionName(String javascriptDocumentTransformFunctionName);
  }
}
