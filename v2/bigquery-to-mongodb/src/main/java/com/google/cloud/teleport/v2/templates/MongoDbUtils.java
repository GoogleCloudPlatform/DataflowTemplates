/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.IOException;
import java.io.Serializable;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
//import com.google.cloud.secretmanager.v1.ProjectName;
//import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
//import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretsPagedResponse;


/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class MongoDbUtils implements Serializable{

    /** Options for Reading MongoDb Documents. */
    public interface  MongoDbOptions extends PipelineOptions, DataflowPipelineOptions {

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
    }


    public interface  BigQueryReadOptions extends PipelineOptions, DataflowPipelineOptions {

        @Description("BigQuery Table name to write to")
        @Default.String("bqtable")
        String getInputTableSpec();
        void setInputTableSpec(String inputTableSpec);
    }

}