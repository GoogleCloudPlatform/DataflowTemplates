/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Templated pipeline to read text from TextIO, apply a javascript UDF to it, and write it to GCS.
 */
public class TextIOToBigQuery {

  /** Options supported by {@link TextIOToBigQuery}. */
  public interface Options extends DataflowPipelineOptions, JavascriptTextTransformerOptions {
    @Description("The GCS location of the text you'd like to process")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("JSON file with BigQuery Schema description")
    ValueProvider<String> getJSONPath();

    void setJSONPath(ValueProvider<String> value);

    @Description("Output topic to write to")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(ValueProvider<String> jsTransformPath);

    @Validation.Required
    @Description("UDF Javascript Function Name")
    ValueProvider<String> getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(
        ValueProvider<String> javascriptTextTransformFunctionName);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TextIOToBigQuery.class);

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";
  private static final String FIELDS = "fields";

  public static List<TableFieldSchema> getFields(JSONArray bqSchemaJsonArray) {

    TableSchema tableSchema = new TableSchema();
    SchemaParser schemaParser = new SchemaParser();
    JSONObject jsonSchema;
    List<TableFieldSchema> fields = new ArrayList<>();

    for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
      JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
      TableFieldSchema field =
          new TableFieldSchema()
              .setName(inputField.getString(NAME))
              .setType(inputField.getString(TYPE));

      if (inputField.has(MODE)) {
        field.setMode(inputField.getString(MODE));
      }

      if (inputField.has(FIELDS)) {
        JSONArray subFieldsJson = inputField.getJSONArray(FIELDS);
        List<TableFieldSchema> subFields = getFields(subFieldsJson);
        field.setFields(subFields);
      }

      fields.add(field);
    }
    return fields;
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())
        .apply(BigQueryConverters.jsonToTableRow())
        .apply(
            "Insert into Bigquery",
            BigQueryIO.writeTableRows()
                .withSchema(
                    NestedValueProvider.of(
                        options.getJSONPath(),
                        new SerializableFunction<String, TableSchema>() {

                          @Override
                          public TableSchema apply(String jsonPath) {

                            TableSchema tableSchema = new TableSchema();
                            SchemaParser schemaParser = new SchemaParser();
                            JSONObject jsonSchema;

                            try {

                              jsonSchema = schemaParser.parseSchema(jsonPath);

                              JSONArray bqSchemaJsonArray =
                                  jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                              tableSchema.setFields(getFields(bqSchemaJsonArray));

                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                            return tableSchema;
                          }
                        }))
                .to(options.getOutputTable())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    pipeline.run();
  }
}
