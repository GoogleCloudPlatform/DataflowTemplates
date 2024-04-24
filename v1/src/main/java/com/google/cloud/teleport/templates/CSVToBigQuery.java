/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.CSVToBigQuery.Options;
import com.google.cloud.teleport.templates.common.CsvConverters;
import com.google.cloud.teleport.templates.common.CsvConverters.CsvPipelineOptions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Templated pipeline to read CSV files from Cloud Storage, and write it to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_CSV_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_CSV_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "CSV Files on Cloud Storage to BigQuery",
    description =
        "The Cloud Storage CSV to BigQuery pipeline is a batch pipeline that allows you to read CSV files stored in "
            + "Cloud Storage, and append the result to a BigQuery table. The CSV files can be uncompressed or compressed in formats listed in https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/Compression.html.",
    optionsClass = Options.class,
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
    })
public class CSVToBigQuery {

  /** Options supported by {@link CSVToBigQuery}. */
  public interface Options extends DataflowPipelineOptions, CsvPipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        description = "Cloud Storage Input File(s)",
        helpText = "Path of the file pattern glob to read from.",
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        example = "gs://your-bucket/path/*.csv")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @TemplateParameter.GcsReadFile(
        order = 2,
        groupName = "Target",
        description = "Cloud Storage location of your BigQuery schema file, described as a JSON",
        helpText =
            "JSON file with BigQuery Schema description. JSON Example: {\n"
                + "\t\"BigQuery Schema\": [\n"
                + "\t\t{\n"
                + "\t\t\t\"name\": \"location\",\n"
                + "\t\t\t\"type\": \"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"name\": \"name\",\n"
                + "\t\t\t\"type\": \"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"name\": \"age\",\n"
                + "\t\t\t\"type\": \"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"name\": \"color\",\n"
                + "\t\t\t\"type\": \"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"name\": \"coffee\",\n"
                + "\t\t\t\"type\": \"STRING\"\n"
                + "\t\t}\n"
                + "\t]\n"
                + "}")
    ValueProvider<String> getSchemaJSONPath();

    void setSchemaJSONPath(ValueProvider<String> value);

    @TemplateParameter.BigQueryTable(
        order = 3,
        groupName = "Target",
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input objects.")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        groupName = "Target",
        description = "Temporary directory for BigQuery loading process",
        helpText = "Temporary directory for BigQuery loading process",
        example = "gs://your-bucket/your-files/temp_dir")
    @Validation.Required
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @TemplateParameter.BigQueryTable(
        order = 5,
        groupName = "Target",
        description = "BigQuery output table for bad records",
        helpText =
            "BigQuery table location to write the bad record. The table's schema must match the {RawContent: STRING, ErrorMsg:STRING}")
    ValueProvider<String> getBadRecordsOutputTable();

    void setBadRecordsOutputTable(ValueProvider<String> value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(CSVToBigQuery.class);

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";
  private static final String RECORD_TYPE = "RECORD";
  private static final String FIELDS_ENTRY = "fields";

  /** The tag for the headers of the CSV if required. */
  private static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  private static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** The tag for the line of the CSV that matches destination table schema. */
  private static final TupleTag<TableRow> GOOD_RECORDS = new TupleTag<TableRow>() {};

  /** The tag for the lines of the CSV that does not match destination table schema. */
  private static final TupleTag<TableRow> BAD_RECORDS = new TupleTag<TableRow>() {};

  /** The schema of the BigQuery table for the bad records. */
  private static final TableSchema errorTableSchema =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("RawContent").setType("STRING"),
                  new TableFieldSchema().setName("ErrorMsg").setType("STRING")));

  private static class StringToTableRowFn extends DoFn<String, TableRow> {
    private final ValueProvider<String> delimiter;
    private final NestedValueProvider<List<String>, String> fields;

    public StringToTableRowFn(
        NestedValueProvider<List<String>, String> schemaFields, ValueProvider<String> delimiter) {
      this.delimiter = delimiter;
      this.fields = schemaFields;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IllegalArgumentException {
      TableRow outputTableRow = new TableRow();
      String[] rowValue =
          Splitter.on(delimiter.get()).splitToList(context.element()).toArray(new String[0]);
      if (rowValue.length != fields.get().size()) {
        LOG.error("Number of fields in the schema and number of Csv headers do not match.");
        outputTableRow.set("RawContent", String.join(delimiter.get(), rowValue));
        outputTableRow.set(
            "ErrorMsg", "Number of fields in the schema and number of Csv headers do not match.");
        context.output(BAD_RECORDS, outputTableRow);
      } else {
        for (int i = 0; i < fields.get().size(); ++i) {
          outputTableRow.set(fields.get().get(i), rowValue[i]);
        }
        context.output(GOOD_RECORDS, outputTableRow);
      }
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollectionTuple tableRows =
        pipeline
            .apply(
                "ReadCsvFile",
                CsvConverters.ReadCsv.newBuilder()
                    .setInputFileSpec(options.getInputFilePattern())
                    .setHasHeaders(options.getContainsHeaders())
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
            .get(CSV_LINES)
            .apply(
                "ConvertToTableRow",
                ParDo.of(
                        new StringToTableRowFn(
                            NestedValueProvider.of(
                                options.getSchemaJSONPath(),
                                jsonPath -> {
                                  List<String> fields = new ArrayList<>();
                                  SchemaParser schemaParser = new SchemaParser();

                                  try {
                                    JSONObject jsonSchema = schemaParser.parseSchema(jsonPath);
                                    JSONArray bqSchemaJsonArray =
                                        jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                                    for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                      JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                      fields.add(inputField.getString(NAME));
                                    }

                                  } catch (Exception e) {
                                    throw new RuntimeException(
                                        "Error parsing schema " + jsonPath, e);
                                  }
                                  return fields;
                                }),
                            options.getDelimiter()))
                    .withOutputTags(GOOD_RECORDS, TupleTagList.of(BAD_RECORDS)));

    tableRows
        .get(GOOD_RECORDS)
        .apply(
            "Insert good records into Bigquery",
            BigQueryIO.writeTableRows()
                .withSchema(
                    NestedValueProvider.of(
                        options.getSchemaJSONPath(),
                        schemaPath -> {
                          TableSchema tableSchema = new TableSchema();
                          List<TableFieldSchema> fields = new ArrayList<>();
                          SchemaParser schemaParser = new SchemaParser();

                          try {
                            JSONObject jsonSchema = schemaParser.parseSchema(schemaPath);
                            JSONArray bqSchemaJsonArray = jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                            for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                              JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                              fields.add(convertToTableFieldSchema(inputField));
                            }
                            tableSchema.setFields(fields);

                          } catch (Exception e) {
                            throw new RuntimeException("Error parsing schema " + schemaPath, e);
                          }
                          return tableSchema;
                        }))
                .to(options.getOutputTable())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    tableRows
        .get(BAD_RECORDS)
        .apply(
            "Insert bad records into Bigquery",
            BigQueryIO.writeTableRows()
                .withSchema(errorTableSchema)
                .to(options.getBadRecordsOutputTable())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    pipeline.run();
  }

  /**
   * Convert a JSONObject from the Schema JSON to a TableFieldSchema. In case of RECORD, it handles
   * the conversion recursively.
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
}
