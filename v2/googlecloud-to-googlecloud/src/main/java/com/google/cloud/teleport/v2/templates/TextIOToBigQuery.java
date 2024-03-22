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
import com.google.cloud.teleport.metadata.MultiTemplate;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer.PythonExternalTextTransformerOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Templated pipeline to read text from TextIO, apply a javascript UDF to it, and write it to GCS.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_GCS_Text_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@MultiTemplate({
  @Template(
      name = "GCS_Text_to_BigQuery_Flex",
      category = TemplateCategory.BATCH,
      displayName = "Text Files on Cloud Storage to BigQuery with BigQuery Storage API support",
      description =
          "The Cloud Storage Text to BigQuery pipeline is a batch pipeline that allows you to read text files stored in "
              + "Cloud Storage, transform them using a JavaScript User Defined Function (UDF) that you provide, and append the result to a BigQuery table.",
      optionsClass = TextIOToBigQuery.Options.class,
      skipOptions = {
        "javascriptTextTransformReloadIntervalMinutes",
        "pythonExternalTextTransformGcsPath",
        "pythonExternalTextTransformFunctionName"
      },
      documentation =
          "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-bigquery",
      flexContainerName = "text-to-bigquery",
      contactInformation = "https://cloud.google.com/support",
      requirements = {
        "Create a JSON file that describes your {{bigquery_name_short}} schema.\n"
            + "    <p>Ensure that there is a top-level JSON array titled <code>BigQuery Schema</code> and that its\n"
            + "      contents follow the pattern <code>{\"name\": \"COLUMN_NAME\", \"type\": \"DATA_TYPE\"}</code>.</p>\n"
            + "    <p>The following JSON describes an example BigQuery schema:</p>\n"
            + "<pre class=\"prettyprint lang-json\">\n"
            + "{\n"
            + "  \"BigQuery Schema\": [\n"
            + "    {\n"
            + "      \"name\": \"location\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"color\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"coffee\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    }\n"
            + "  ]\n"
            + "}\n"
            + "</pre>",
        "Create a JavaScript (<code>.js</code>) file with your UDF function that supplies the logic\n"
            + "    to transform the lines of text. Your function must return a JSON string.\n"
            + "    <p>For example, this function splits each line of a CSV file and returns a JSON string after\n"
            + "      transforming the values.</p>\n"
            + "<pre class=\"prettyprint\" suppresswarning>\n"
            + "function transform(line) {\n"
            + "var values = line.split(',');\n"
            + "\n"
            + "var obj = new Object();\n"
            + "obj.location = values[0];\n"
            + "obj.name = values[1];\n"
            + "obj.age = values[2];\n"
            + "obj.color = values[3];\n"
            + "obj.coffee = values[4];\n"
            + "var jsonString = JSON.stringify(obj);\n"
            + "\n"
            + "return jsonString;\n"
            + "}</pre>"
      }),
  @Template(
      name = "GCS_Text_to_BigQuery_Xlang",
      category = TemplateCategory.BATCH,
      displayName =
          "Text Files on Cloud Storage to BigQuery with BigQuery Storage API & Python UDF support",
      type = Template.TemplateType.XLANG,
      description =
          "The Cloud Storage Text to BigQuery pipeline is a batch pipeline that allows you to read text files stored in "
              + "Cloud Storage, transform them using a Python User Defined Function (UDF) that you provide, and append the result to a BigQuery table.",
      optionsClass = TextIOToBigQuery.Options.class,
      skipOptions = {
        "javascriptTextTransformReloadIntervalMinutes",
        "javascriptTextTransformGcsPath",
        "javascriptTextTransformFunctionName"
      },
      optionalOptions = {"javascriptTextTransformGcsPath", "javascriptTextTransformFunctionName"},
      documentation =
          "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-bigquery",
      flexContainerName = "text-to-bigquery",
      contactInformation = "https://cloud.google.com/support",
      requirements = {
        "Create a JSON file that describes your {{bigquery_name_short}} schema.\n"
            + "    <p>Ensure that there is a top-level JSON array titled <code>BigQuery Schema</code> and that its\n"
            + "      contents follow the pattern <code>{\"name\": \"COLUMN_NAME\", \"type\": \"DATA_TYPE\"}</code>.</p>\n"
            + "    <p>The following JSON describes an example BigQuery schema:</p>\n"
            + "<pre class=\"prettyprint lang-json\">\n"
            + "{\n"
            + "  \"BigQuery Schema\": [\n"
            + "    {\n"
            + "      \"name\": \"location\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"color\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"coffee\",\n"
            + "      \"type\": \"STRING\"\n"
            + "    }\n"
            + "  ]\n"
            + "}\n"
            + "</pre>",
        "Create a JavaScript (<code>.js</code>) file with your UDF function that supplies the logic\n"
            + "    to transform the lines of text. Your function must return a JSON string.\n"
            + "    <p>For example, this function splits each line of a CSV file and returns a JSON string after\n"
            + "      transforming the values.</p>\n"
            + "<pre class=\"prettyprint\" suppresswarning>\n"
            + "function transform(line) {\n"
            + "var values = line.split(',');\n"
            + "\n"
            + "var obj = new Object();\n"
            + "obj.location = values[0];\n"
            + "obj.name = values[1];\n"
            + "obj.age = values[2];\n"
            + "obj.color = values[3];\n"
            + "obj.coffee = values[4];\n"
            + "var jsonString = JSON.stringify(obj);\n"
            + "\n"
            + "return jsonString;\n"
            + "}</pre>"
      })
})
public class TextIOToBigQuery {

  /** Options supported by {@link TextIOToBigQuery}. */
  public interface Options
      extends DataflowPipelineOptions,
          PythonExternalTextTransformerOptions,
          BigQueryStorageApiBatchOptions {
    @TemplateParameter.Text(
        order = 1,
        optional = false,
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        description = "The GCS location of the text you'd like to process",
        helpText = "The path to the Cloud Storage text to read.",
        example = "gs://your-bucket/your-file.txt")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.GcsReadFile(
        order = 2,
        optional = false,
        description = "JSON file with BigQuery Schema description",
        helpText = "The Cloud Storage path to the JSON file that defines your BigQuery schema.",
        example = "gs://your-bucket/your-schema.json")
    String getJSONPath();

    void setJSONPath(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        regexes = {".+:.+\\..+"},
        description = "Output table to write to",
        helpText =
            "The location of the BigQuery table in which to store your processed data. If you reuse"
                + " an existing table, it will be overwritten.",
        example = "your-project:your-dataset.your-table")
    String getOutputTable();

    void setOutputTable(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = false,
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        description = "GCS path to javascript fn for transforming output",
        helpText =
            "The Cloud Storage path pattern for the JavaScript code containing your user-defined"
                + " functions.",
        example = "gs://your-bucket/your-transforms/*.js")
    String getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(String jsTransformPath);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        regexes = {"[a-zA-Z0-9_]+"},
        description = "UDF Javascript Function Name",
        helpText =
            "The name of the function to call from your JavaScript file. Use only letters, digits,"
                + " and underscores.",
        example = "transform_udf1")
    String getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);

    @Validation.Required
    @TemplateParameter.GcsWriteFolder(
        order = 6,
        optional = false,
        description = "Temporary directory for BigQuery loading process",
        helpText = "Temporary directory for the BigQuery loading process.",
        example = "gs://your-bucket/your-files/temp-dir")
    String getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(String directory);
  }

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";
  private static final String RECORD_TYPE = "RECORD";
  private static final String FIELDS_ENTRY = "fields";

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options, () -> writeToBQTransform(options));
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @param writeToBQ the transform that outputs {@link TableRow}s to BigQuery.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(Options options, Supplier<Write<TableRow>> writeToBQ) {
    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    Pipeline pipeline = Pipeline.create(options);

    boolean useJavascriptUdf = !Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath());
    boolean usePythonUdf = !Strings.isNullOrEmpty(options.getPythonExternalTextTransformGcsPath());
    if (useJavascriptUdf == usePythonUdf) {
      throw new IllegalArgumentException(
          "Either javascript or Python gcs path must be provided, but not both.");
    }

    PCollection<String> source =
        pipeline.apply("Read from source", TextIO.read().from(options.getInputFilePattern()));
    PCollection<TableRow> udfOut;

    if (usePythonUdf) {
      udfOut =
          source
              .apply(
                  "MapToRecord",
                  PythonExternalTextTransformer.FailsafeRowPythonExternalUdf
                      .stringMappingFunction())
              .setCoder(
                  RowCoder.of(
                      PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.ROW_SCHEMA))
              .apply(
                  "InvokeUDF",
                  PythonExternalTextTransformer.FailsafePythonExternalUdf.newBuilder()
                      .setFileSystemPath(options.getPythonExternalTextTransformGcsPath())
                      .setFunctionName(options.getPythonExternalTextTransformFunctionName())
                      .build())
              .setCoder(
                  RowCoder.of(
                      PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.FAILSAFE_SCHEMA))
              .apply(
                  MapElements.via(
                      new SimpleFunction<Row, TableRow>() {
                        @Override
                        public TableRow apply(Row row) {
                          String errorMessage = row.getValue("error_message");
                          String stackTrace = row.getValue("stack_trace");
                          if (!Strings.isNullOrEmpty(errorMessage)
                              || !Strings.isNullOrEmpty(stackTrace)) {
                            Object originalElement = row.getValue("original");
                            throw new RuntimeException(
                                String.format(
                                    "Error applying UDF to the source record [%s], error message: [%s], stack trace: [%s]",
                                    originalElement, errorMessage, stackTrace));
                          }

                          Row transformedRow = row.getValue("transformed");
                          return BigQueryConverters.convertJsonToTableRow(
                              transformedRow.getValue("message"));
                        }
                      }));
    } else {
      udfOut =
          source
              .apply(
                  TransformTextViaJavascript.newBuilder()
                      .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                      .setFunctionName(options.getJavascriptTextTransformFunctionName())
                      .setReloadIntervalMinutes(
                          options.getJavascriptTextTransformReloadIntervalMinutes())
                      .build())
              .apply(
                  MapElements.via(
                      new SimpleFunction<String, TableRow>() {
                        @Override
                        public TableRow apply(String json) {
                          return BigQueryConverters.convertJsonToTableRow(json);
                        }
                      }));
    }

    udfOut.apply("Insert into Bigquery", writeToBQ.get());

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

    JSONObject jsonSchema = parseJson(jsonPath);

    JSONArray bqSchemaJsonArray = jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

    for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
      JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
      fields.add(convertToTableFieldSchema(inputField));
    }
    tableSchema.setFields(fields);

    return tableSchema;
  }

  /**
   * Convert a JSONObject from the Schema JSON to a TableFieldSchema. In case of RECORD, it handles
   * it recursively.
   *
   * @param inputField Input field to convert.
   * @return TableFieldSchema instance to populate the schema.
   */
  private static TableFieldSchema convertToTableFieldSchema(JSONObject inputField) {
    TableFieldSchema field =
        new TableFieldSchema()
            .setName(inputField.getString(NAME))
            .setType(inputField.getString(TYPE));

    if (inputField.has(MODE)) {
      field.setMode(inputField.getString(MODE));
    }

    if (inputField.getString(TYPE) != null && inputField.getString(TYPE).equals(RECORD_TYPE)) {
      List<TableFieldSchema> nestedFields = new ArrayList<>();
      JSONArray fieldsArr = inputField.getJSONArray(FIELDS_ENTRY);
      for (int i = 0; i < fieldsArr.length(); i++) {
        JSONObject nestedJSON = fieldsArr.getJSONObject(i);
        nestedFields.add(convertToTableFieldSchema(nestedJSON));
      }
      field.setFields(nestedFields);
    }

    return field;
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
      // accessing GCS needs to be done after the pipeline create call, otherwise FileSystems
      // doesn't know about GCS.
      ReadableByteChannel readableByteChannel =
          FileSystems.open(FileSystems.matchNewResource(pathToJson, false));
      String json =
          new String(
              StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)),
              StandardCharsets.UTF_8);
      return new JSONObject(json);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
