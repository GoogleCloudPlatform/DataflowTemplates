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
package com.google.cloud.teleport.v2.mongodb.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bson.Document;

/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class MongoDbUtils implements Serializable {

  /**
   * Returns the Table schema for BiQquery table based on user input The tabble schema can be a 3
   * column table with _id, document as a Json string and timestamp by default Or the Table schema
   * can be flattened version of the document with each field as a column for userOption "FLATTEN".
   */
  public static TableSchema getTableFieldSchema(
      String uri, String database, String collection, String userOption) {
    List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();
    if (userOption.equals("FLATTEN")) {
      Document document = getMongoDbDocument(uri, database, collection);
      document.forEach(
          (key, value) -> {
            bigquerySchemaFields.add(
                new TableFieldSchema()
                    .setName(key)
                    .setType(getTableSchemaDataType(value.getClass().getName())));
          });
    } else {
      bigquerySchemaFields.add(new TableFieldSchema().setName("id").setType("STRING"));
      bigquerySchemaFields.add(new TableFieldSchema().setName("source_data").setType("STRING"));
      bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    }
    TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
    return bigquerySchema;
  }

  /**
   * Generates and returns the tablerow for the mongodb document based on user input. The table row
   * will have a column with whole document as a json string by default. If user passes "FLATTEN" to
   * userOption, The document will be flattned for first l evel and stored into BigQuery.
   */
  public static TableRow generateTableRow(Document document, String userOption) {
    TableRow row = new TableRow();
    if (userOption.equals("FLATTEN")) {
      document.forEach(
          (key, value) -> {
            String valueClass = value.getClass().getName();
            switch (valueClass) {
              case "org.bson.types.ObjectId":
                row.set(key, value.toString());
                break;
              case "org.bson.Document":
                row.set(key, value.toString());
                break;
              case "java.lang.Double":
                row.set(key, value);
                break;
              case "java.util.Integer":
                row.set(key, value);
                break;
              case "java.util.ArrayList":
                row.set(key, value.toString());
                break;
              default:
                row.set(key, value.toString());
            }
          });
    } else {
      String sourceData = document.toJson();
      DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
      LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));

      row.set("id", document.getObjectId("_id").toString())
          .set("source_data", sourceData)
          .set("timestamp", localdate.format(timeFormat));
    }
    return row;
  }

  public static TableRow generateTableRowFromJson(
      HashMap<String, Object> document, String userOption) {
    TableRow row = new TableRow();
    if (userOption.equals("FLATTEN")) {
      document.forEach(
          (key, value) -> {
            String valueClass = value.getClass().getName();
            switch (valueClass) {
              case "org.bson.types.ObjectId":
                row.set(key, value.toString());
                break;
              case "org.bson.Document":
                row.set(key, value.toString());
                break;
              case "java.lang.Double":
                row.set(key, value);
                break;
              case "java.util.Integer":
                row.set(key, value);
                break;
              case "java.util.ArrayList":
                row.set(key, value.toString());
                break;
              default:
                row.set(key, value.toString());
            }
          });
    } else {
      String sourceData = document.toString();
      DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
      LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));

      row.set("id", document.get("_id").toString())
          .set("source_data", sourceData)
          .set("timestamp", localdate.format(timeFormat));
    }
    return row;
  }

  /** Maps and Returns the Datatype form MongoDb To BigQuery. */
  public static String getTableSchemaDataType(String s) {
    switch (s) {
      case "java.lang.Integer":
        return "INTEGER";
      case "java.lang.Boolean":
        return "BOOLEAN";
      case "org.bson.Document":
        return "STRING";
      case "java.lang.Double":
        return "FLOAT";
      case "java.util.ArrayList":
        return "STRING";
      case "org.bson.types.ObjectId":
        return "STRING";
    }
    return "STRING";
  }

  /** Get a Document from MongoDB to generate the schema for BigQuery. */
  public static Document getMongoDbDocument(String uri, String dbName, String collName) {
    MongoClient mongoClient = MongoClients.create(uri);
    MongoDatabase database = mongoClient.getDatabase(dbName);
    MongoCollection<Document> collection = database.getCollection(collName);
    Document doc = collection.find().first();
    return doc;
  }
}
