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
        helpText = "URI to connect to MongoDB Atlas.")
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
        enumOptions = {"FLATTEN", "NONE"},
        description = "User option",
        helpText =
            "User option: FLATTEN or NONE. FLATTEN will flatten the documents for 1 level. NONE will store the whole document as json string.")
    String getUserOption();

    void setUserOption(String userOption);
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
            "BigQuery table location to write the output to. The name should be in the format <project>:<dataset>.<table_name>. The table's schema must match input objects.")
    @Default.String("bqtable")
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);
  }
}
