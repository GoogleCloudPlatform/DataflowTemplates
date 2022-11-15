/*
 * Copyright (C) 2022 Google LLC
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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.common.annotations.VisibleForTesting;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Templated pipeline to read text from TextIO, apply a javascript UDF to it, and write it to GCS.
 */
public class TextIOToBigQuery {

  /** Options supported by {@link TextIOToBigQuery}. */
  public interface Options
      extends DataflowPipelineOptions, JavascriptTextTransformerOptions, BigQueryOptions {
    @Description("The GCS location of the text you'd like to process")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @Description("JSON file with BigQuery Schema description")
    String getJSONPath();

    void setJSONPath(String value);

    @Description("Output table to write to")
    String getOutputTable();

    void setOutputTable(String value);

    @Description("GCS path to javascript fn for transforming output")
    String getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(String jsTransformPath);

    @Validation.Required
    @Description("UDF Javascript Function Name")
    String getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    String getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(String directory);
  }

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options, writeToBQTransform(options));
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @param writeToBQ the transform that outputs {@link TableRow}s to BigQuery.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(Options options, Write<TableRow> writeToBQ) {
    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())
        .apply(
            MapElements.via(
                new SimpleFunction<String, TableRow>() {
                  @Override
                  public TableRow apply(String json) {
                    return BigQueryConverters.convertJsonToTableRow(json);
                  }
                }))
        .apply("Insert into Bigquery", writeToBQ);

    return pipeline.run();
  }

  /** Create the {@link Write} transform that outputs the collection to BigQuery. */
  @VisibleForTesting
  static Write<TableRow> writeToBQTransform(Options options) {
    return BigQueryIO.writeTableRows()
        .withSchema(parseSchema(options.getJSONPath()))
        .to(options.getOutputTable())
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withCustomGcsTempLocation(
            StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory()));
  }

  /** Parse BigQuery schema from a Json file. */
  private static TableSchema parseSchema(String jsonPath) {
    TableSchema tableSchema = new TableSchema();
    List<TableFieldSchema> fields = new ArrayList<>();
    JSONObject jsonSchema;

    try {

      jsonSchema = parseJson(jsonPath);

      JSONArray bqSchemaJsonArray = jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

      for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
        JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
        TableFieldSchema field =
            new TableFieldSchema()
                .setName(inputField.getString(NAME))
                .setType(inputField.getString(TYPE));

        if (inputField.has(MODE)) {
          field.setMode(inputField.getString(MODE));
        }

        fields.add(field);
      }
      tableSchema.setFields(fields);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return tableSchema;
  }

  /**
   * Parses a JSON file and returns a JSONObject containing the necessary source, sink, and schema
   * information.
   *
   * @param pathToJson the JSON file location so we can download and parse it
   * @return the parsed JSONObject
   */
  private static JSONObject parseJson(String pathToJson) {
    try {
      ReadableByteChannel readableByteChannel =
          FileSystems.open(FileSystems.matchNewResource(pathToJson, false));
      String json =
          new String(
              StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)));
      return new JSONObject(json);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
