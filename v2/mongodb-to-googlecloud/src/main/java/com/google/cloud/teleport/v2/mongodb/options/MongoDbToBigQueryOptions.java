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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link MongoDbToBigQueryOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public class MongoDbToBigQueryOptions {

  /** Options for Reading MongoDb Documents. */
  public interface MongoDbOptions extends PipelineOptions, DataflowPipelineOptions {
    @Description("MongoDB URI for connecting to MongoDB Cluster")
    @Default.String("mongouri")
    String getMongoDbUri();

    void setMongoDbUri(String getMongoDbUri);

    @Description("MongoDb Database name to read the data from")
    @Default.String("db")
    String getDatabase();

    void setDatabase(String database);

    @Description("MongoDb collection to read the data from")
    @Default.String("collection")
    String getCollection();

    void setCollection(String collection);

    @Description("MongoDb collection to read the data from")
    String getUserOption();

    void setUserOption(String userOption);
  }

  /** Options for reading from PubSub. */
  public interface PubSubOptions extends PipelineOptions, DataflowPipelineOptions {
    @Description("MongoDb collection to read the data from")
    String getInputTopic();

    void setInputTopic(String inputTopic);
  }

  /** Options for Reading BigQuery Rows. */
  public interface BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {

    @Description("BigQuery Table name to write to")
    @Default.String("bqtable")
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);
  }
}
