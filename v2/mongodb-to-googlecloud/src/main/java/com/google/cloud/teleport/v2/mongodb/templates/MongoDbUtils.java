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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class MongoDbUtils implements Serializable {

  /**
   * Returns the Table schema for BiQquery table based on user input The tabble schema can be a 3
   * column table with _id, document as a Json string and timestamp by default Or the Table schema
   * can be flattened version of the document with each field as a column for userOption "FLATTEN".
   */
  static final DateTimeFormatter TIMEFORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToBigQuery.class);

  static final Gson GSON = new Gson();

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
      bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    } else {
      bigquerySchemaFields.add(new TableFieldSchema().setName("id").setType("STRING"));
      bigquerySchemaFields.add(new TableFieldSchema().setName("source_data").setType("STRING"));
      bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    }
    TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
    return bigquerySchema;
  }

  /** Maps and Returns the Datatype form MongoDb To BigQuery. */
  public static String getTableSchemaDataType(String s) {
    switch (s) {
      case "java.lang.Boolean":
        return "BOOLEAN";
      case "java.lang.Double":
        return "FLOAT";
      case "java.lang.Long":
      case "java.lang.Integer":
        return "INT64";
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

  public static TableRow getTableSchema(Document document, String userOption) {
    TableRow row = new TableRow();
    LOG.info("Document" + document);
    if (userOption.equals("FLATTEN") || userOption.equals("UDF")) {
      document.forEach(
          (key, value) -> {
            String valueClass = value.getClass().getName();
            switch (valueClass) {
              case "java.lang.Double":
              case "java.lang.Integer":
              case "java.lang.Long":
              case "java.lang.Boolean":
                row.set(key, value);
                break;
              case "org.bson.Document":
                String data = GSON.toJson(value);
                row.set(key, data);
                break;
              default:
                row.set(key, value.toString());
            }
          });
      LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
      row.set("timestamp", localdate.format(TIMEFORMAT));
    } else {
      LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
      String sourceData = GSON.toJson(document);
      row.set("id", document.get("_id").toString())
          .set("source_data", sourceData)
          .set("timestamp", localdate.format(TIMEFORMAT));
    }
    return row;
  }

  public static TableSchema getTableFieldSchemaForUDF(
      String uri, String database, String collection, String udfGcsPath, String udfFunctionName)
      throws IOException, ScriptException, NoSuchMethodException {
    Document document = getMongoDbDocument(uri, database, collection);
    List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();

    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    Collection<String> scripts = getScripts(udfGcsPath);

    Invocable invocable = newInvocable(scripts);
    if (invocable == null) {
      throw new RuntimeException("No udf was loaded");
    }

    Object result = invocable.invokeFunction(udfFunctionName, document);
    Document doc = (Document) result;
    doc.forEach(
        (key, value) -> {
          bigquerySchemaFields.add(
              new TableFieldSchema()
                  .setName(key)
                  .setType(getTableSchemaDataType(value.getClass().getName())));
        });
    bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
    return bigquerySchema;
  }

  private static Collection<String> getScripts(String path) throws IOException {
    MatchResult result = FileSystems.match(path);
    checkArgument(
        result.status() == Status.OK && !result.metadata().isEmpty(),
        "Failed to match any files with the pattern: " + path);

    List<String> scripts =
        result.metadata().stream()
            .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
            .map(Metadata::resourceId)
            .map(
                resourceId -> {
                  try (Reader reader =
                      Channels.newReader(
                          FileSystems.open(resourceId), StandardCharsets.UTF_8.name())) {
                    return CharStreams.toString(reader);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(Collectors.toList());
    return scripts;
  }

  @Nullable
  private static Invocable newInvocable(Collection<String> scripts) throws ScriptException {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("JavaScript");

    if (engine == null) {
      List<String> availableEngines = new ArrayList<>();
      for (ScriptEngineFactory factory : manager.getEngineFactories()) {
        availableEngines.add(factory.getEngineName() + " " + factory.getEngineVersion());
      }
      throw new RuntimeException(
          String.format("JavaScript engine not available. Found engines: %s.", availableEngines));
    }

    for (String script : scripts) {
      engine.eval(script);
    }

    return (Invocable) engine;
  }
}
