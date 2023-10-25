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
        description = "MongoDB Connection URI",
        helpText = "MongoDB connection URI in the format `mongodb+srv://:@`.")
    @Default.String("mongouri")
    String getMongoDbUri();

    void setMongoDbUri(String getMongoDbUri);

    @TemplateParameter.Text(
        order = 2,
        description = "MongoDB database",
        helpText = "Database in MongoDB to read the collection from.",
        example = "my-db")
    String getDatabase();

    void setDatabase(String database);

    @TemplateParameter.Text(
        order = 3,
        description = "MongoDB collection",
        helpText = "Name of the collection inside MongoDB database.",
        example = "my-collection")
    @Default.String("collection")
    String getCollection();

    void setCollection(String collection);

    @TemplateParameter.Enum(
        order = 4,
        enumOptions = {@TemplateEnumOption("FLATTEN"), @TemplateEnumOption("NONE")},
        description = "User option",
        helpText =
            "User option: `FLATTEN` or `NONE`. `FLATTEN` flattens the documents to the single level. `NONE` stores the whole document as a JSON string.")
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
  }

  /** Options for reading from PubSub. */
  public interface PubSubOptions extends PipelineOptions, DataflowPipelineOptions {
    @TemplateParameter.PubsubTopic(
        order = 1,
        description = "Pub/Sub input topic",
        helpText =
            "Pub/Sub topic to read the input from, in the format of "
                + "'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    String getInputTopic();

    void setInputTopic(String inputTopic);
  }

  /** Options for Reading BigQuery Rows. */
  public interface BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.")
    @Default.String("bqtable")
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);
  }

  /** UDF options. */
  public interface JavascriptDocumentTransformerOptions extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = true,
        description = "JavaScript UDF path in Cloud Storage.",
        helpText =
            "The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.",
        example = "gs://your-bucket/your-transforms/*.js")
    String getJavascriptDocumentTransformGcsPath();

    void setJavascriptDocumentTransformGcsPath(String javascriptDocumentTransformGcsPath);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "The name of the JavaScript function to call as your UDF.",
        helpText =
            "The function name should only contain letters, digits and underscores. Example: 'transform' or 'transform_udf1'.",
        example = "transform")
    String getJavascriptDocumentTransformFunctionName();

    void setJavascriptDocumentTransformFunctionName(String javascriptDocumentTransformFunctionName);
  }
}
