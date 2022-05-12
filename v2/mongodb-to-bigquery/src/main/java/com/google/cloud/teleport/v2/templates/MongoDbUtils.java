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

        @Description("MongoDb collection to read the data from")
        String getUserOption();
        void setUserOption(String userOption);
    }

    public interface  BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {

        @Description("BigQuery Table name to write to")
        @Default.String("bqtable")
        String getOutputTableSpec();
        void setOutputTableSpec(String outputTableSpec);
    }

    /** Returns the Table schema for Bigquery table based on user input
     * The tabble schema can be a 3 column table with _id, document as a Json string and timestamp by default
     * Or the Table schema can be flattened version of the document with each field as a column
     * for userOption "FLATTEN"*/
    public static TableSchema getTableFieldSchema(String uri, String database, String collection, String userOption){
        List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();
        if(userOption != "FLATTEN"){
            bigquerySchemaFields.add(new TableFieldSchema().setName("id").setType("STRING"));
            bigquerySchemaFields.add(new TableFieldSchema().setName("source_data").setType("STRING"));
            bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        }
        else{
            Document document = getMongoDbDocument(uri, database, collection);
            document.forEach((key, value) ->
            {
                bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType(getTableSchemaDataType(value.getClass().getName())));
            });
        }
        TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
        return bigquerySchema;
    }

    /** Generates and returns the tablerow for the mongodb document based on user input
     * The table row will have a column with whole document as a json string by default
     * If user passes "FLATTEN" to userOption, The document will be flattnered for first leven and stored into BigQuery
     * */
    public static TableRow generateTableRow(Document document, String userOption){
        TableRow row = new TableRow();
        if(userOption != "FLATTEN"){
            String source_data = document.toJson();
            DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));

            row
                    .set("id",document.getObjectId("_id").toString())
                    .set("source_data",source_data)
                    .set("timestamp", localdate.format(time_format));
        }else{
            document.forEach((key, value) ->
            {
                String valueClass = value.getClass().getName();
                switch(valueClass){
                    case "org.bson.types.ObjectId": row.set(key,value.toString());
                    case "org.bson.Document":  row.set(key,value.toString());
                    case "java.lang.Double":  row.set(key,value);
                    case "java.util.Integer":  row.set(key,value);
                    case "java.util.ArrayList":  row.set(key,value.toString());
                    default:row.set(key,value.toString());
                }
            });
        }
        return row;
    }

    /** Maps and Returns the Datatype form MongoDb To BigQuery */
    public static String getTableSchemaDataType(String s){
        switch(s){
            case "java.lang.Integer": return "INTEGER";
            case "java.lang.Boolean" : return "BOOLEAN";
            case "org.bson.Document": return "JSON";
            case "java.lang.Double": return "FLOAT";
            case "java.util.ArrayList": return "STRING";
            case "org.bson.types.ObjectId": return "STRING";
        }
        return "STRING";
    }

    /** Get a Document from MongoDB to generate the schema for Bigquery */
    public static Document getMongoDbDocument(String uri, String dbName, String collName){
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collName);
        Document doc = collection.find().first();
        return doc;
    }

}