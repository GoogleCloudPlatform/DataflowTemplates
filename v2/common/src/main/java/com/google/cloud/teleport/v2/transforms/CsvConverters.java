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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Csv files. */
public class CsvConverters {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(CsvConverters.class);

  private static final String SUCCESSFUL_TO_JSON_COUNTER = "SuccessfulToJsonCounter";

  private static final String FAILED_TO_JSON_COUNTER = "FailedToJsonCounter";

  private static JsonParser jsonParser = new JsonParser();

  /**
   * Builds Json string from list of values and headers or values and schema if schema is provided.
   *
   * @param headers optional list of strings which is the header of the Csv file.
   * @param values list of strings which are combined with header or json schema to create Json
   *     string.
   * @param jsonSchemaString
   * @return Json string containing object.
   * @throws IOException thrown if Json object is not able to be written.
   * @throws NumberFormatException thrown if value cannot be parsed into type successfully.
   */
  static String buildJsonString(
      @Nullable List<String> headers, List<String> values, @Nullable String jsonSchemaString)
      throws Exception {

    StringWriter stringWriter = new StringWriter();
    JsonWriter writer = new JsonWriter(stringWriter);

    if (jsonSchemaString != null) {
      JsonArray jsonSchema = jsonParser.parse(jsonSchemaString).getAsJsonArray();
      writer.beginObject();

      for (int i = 0; i < jsonSchema.size(); i++) {
        JsonObject jsonObject = jsonSchema.get(i).getAsJsonObject();
        String type = jsonObject.get("type").getAsString().toUpperCase();
        writer.name(jsonObject.get("name").getAsString());

        switch (type) {
          case "LONG":
            writer.value(Long.parseLong(values.get(i)));
            break;

          case "DOUBLE":
            writer.value(Double.parseDouble(values.get(i)));
            break;

          case "INTEGER":
            writer.value(Integer.parseInt(values.get(i)));
            break;

          case "SHORT":
            writer.value(Short.parseShort(values.get(i)));
            break;

          case "BYTE":
            writer.value(Byte.parseByte(values.get(i)));
            break;

          case "FLOAT":
            writer.value(Float.parseFloat(values.get(i)));
            break;

          case "TEXT":
          case "KEYWORD":
          case "STRING":
            writer.value(values.get(i));
            break;

          default:
            LOG.error("Invalid data type, got: " + type);
            throw new RuntimeException("Invalid data type, got: " + type);
        }
      }
      writer.endObject();
      writer.close();
      return stringWriter.toString();

    } else if (headers != null) {

      writer.beginObject();

      for (int i = 0; i < headers.size(); i++) {
        writer.name(headers.get(i));
        writer.value(values.get(i));
      }

      writer.endObject();
      writer.close();
      return stringWriter.toString();

    } else {
      LOG.error("No headers or schema specified");
      throw new RuntimeException("No headers or schema specified");
    }
  }

  /**
   * Gets Csv format accoring to <a
   * href="https://javadoc.io/doc/org.apache.commons/commons-csv">Apache Commons CSV</a>. If user
   * passed invalid format error is thrown.
   */
  public static CSVFormat getCsvFormat(String formatString, @Nullable String delimiter) {

    CSVFormat format = CSVFormat.Predefined.valueOf(formatString).getFormat();

    // If a delimiter has been passed set it here.
    if (delimiter != null) {
      return format.withDelimiter(delimiter.charAt(0));
    }
    return format;
  }

  /** Necessary {@link PipelineOptions} options for Csv Pipelines. */
  public interface CsvPipelineOptions extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "The input filepattern to read from.",
        helpText = "Cloud storage file pattern glob to read from. ex: gs://your-bucket/path/*.csv")
    String getInputFileSpec();

    void setInputFileSpec(String inputFileSpec);

    @TemplateParameter.Boolean(
        order = 2,
        optional = true,
        description = "Input CSV files contain a header record.",
        helpText =
            "Input CSV files contain a header record (true/false). Only required if reading CSV files.")
    @Default.Boolean(false)
    Boolean getContainsHeaders();

    void setContainsHeaders(Boolean containsHeaders);

    @TemplateParameter.BigQueryTable(
        order = 3,
        description = "BigQuery Deadletter table to send failed inserts.",
        helpText =
            "Messages failed to reach the target for all kind of reasons (e.g., mismatched schema, malformed json) are written to this table.",
        example = "your-project:your-dataset.your-table-name")
    String getDeadletterTable();

    void setDeadletterTable(String deadletterTable);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "Column delimiter of the data files.",
        helpText =
            "The column delimiter of the input text files. Default: use delimiter provided in csvFormat",
        example = ",")
    @Default.InstanceFactory(DelimiterFactory.class)
    String getDelimiter();

    void setDelimiter(String delimiter);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "CSV Format to use for parsing records.",
        helpText =
            "CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: "
                + "https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html")
    @Default.String("Default")
    String getCsvFormat();

    void setCsvFormat(String csvFormat);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Path to JSON schema",
        helpText = "Path to JSON schema. Default: null.",
        example = "gs://path/to/schema")
    String getJsonSchemaPath();

    void setJsonSchemaPath(String jsonSchemaPath);

    @TemplateParameter.Boolean(
        order = 7,
        optional = true,
        description = "Set to true if number of files is in the tens of thousands",
        helpText = "Set to true if number of files is in the tens of thousands.")
    @Default.Boolean(false)
    Boolean getLargeNumFiles();

    void setLargeNumFiles(Boolean largeNumFiles);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        regexes = {"^(US-ASCII|ISO-8859-1|UTF-8|UTF-16)$"},
        description = "CSV file encoding",
        helpText =
            "CSV file character encoding format. Allowed Values are US-ASCII"
                + ", ISO-8859-1, UTF-8, UTF-16")
    @Default.String("UTF-8")
    String getCsvFileEncoding();

    void setCsvFileEncoding(String csvFileEncoding);

    @TemplateParameter.Boolean(
        order = 9,
        optional = true,
        description = "Log detailed CSV conversion errors",
        helpText =
            "Set to true to enable detailed error logging when CSV parsing fails. Note that this may expose sensitive data in the logs (e.g., if the CSV file contains passwords)."
                + " Default: false.")
    @Default.Boolean(false)
    Boolean getLogDetailedCsvConversionErrors();

    void setLogDetailedCsvConversionErrors(Boolean logDetailedCsvConversionErrors);
  }

  /**
   * Default value factory to get delimiter from Csv format so that if the user does not pass one
   * in, it matches the supplied {@link CsvPipelineOptions#getCsvFormat()}.
   */
  public static class DelimiterFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      CSVFormat csvFormat = getCsvFormat(options.as(CsvPipelineOptions.class).getCsvFormat(), null);
      return String.valueOf(csvFormat.getDelimiter());
    }
  }

  /**
   * The {@link LineToFailsafeJson} interface converts a line from a Csv file into a Json string.
   * Uses either: Javascript Udf, Json schema or the headers of the file to create the Json object
   * which is then added to the {@link FailsafeElement} as the new payload.
   */
  @AutoValue
  public abstract static class LineToFailsafeJson
      extends PTransform<PCollectionTuple, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_CsvConverters_LineToFailsafeJson.Builder();
    }

    public abstract String delimiter();

    @Nullable
    public abstract String udfFileSystemPath();

    @Nullable
    public abstract String udfFunctionName();

    @Nullable
    public abstract Integer udfReloadIntervalMinutes();

    @Nullable
    public abstract String jsonSchemaPath();

    @Nullable
    public abstract String jsonSchema();

    public abstract TupleTag<String> headerTag();

    public abstract TupleTag<String> lineTag();

    public abstract TupleTag<FailsafeElement<String, String>> udfOutputTag();

    public abstract TupleTag<FailsafeElement<String, String>> udfDeadletterTag();

    @Override
    public PCollectionTuple expand(PCollectionTuple lines) {

      PCollectionView<String> headersView = null;

      // Convert csv lines into Failsafe elements so that we can recover over multiple transforms.
      PCollection<FailsafeElement<String, String>> lineFailsafeElements =
          lines
              .get(lineTag())
              .apply("LineToFailsafeElement", ParDo.of(new LineToFailsafeElementFn()));

      // If UDF is specified then use that to parse csv lines.
      if (udfFileSystemPath() != null) {

        return lineFailsafeElements.apply(
            "LineToDocumentUsingUdf",
            FailsafeJavascriptUdf.<String>newBuilder()
                .setFileSystemPath(udfFileSystemPath())
                .setFunctionName(udfFunctionName())
                .setReloadIntervalMinutes(udfReloadIntervalMinutes())
                .setSuccessTag(udfOutputTag())
                .setFailureTag(udfDeadletterTag())
                .build());
      }

      // If no udf then use json schema
      if (jsonSchemaPath() != null || jsonSchema() != null) {
        String schema =
            jsonSchemaPath() != null ? GCSUtils.getGcsFileAsString(jsonSchemaPath()) : jsonSchema();

        return lineFailsafeElements.apply(
            "LineToDocumentUsingSchema",
            ParDo.of(
                    new FailsafeElementToJsonFn(
                        headersView, schema, delimiter(), udfDeadletterTag()))
                .withOutputTags(udfOutputTag(), TupleTagList.of(udfDeadletterTag())));
      }

      // Run if using headers
      if (!lines.has(headerTag())) {
        throw new IllegalArgumentException(
            "Headers are not provided for CSV import. Either provide a JSON schema, or set the 'containsHeader' parameter to true.");
      }
      headersView = lines.get(headerTag()).apply(Sample.any(1)).apply(View.asSingleton());

      PCollectionView<String> finalHeadersView = headersView;
      lines
          .get(headerTag())
          .apply(
              "CheckHeaderConsistency",
              ParDo.of(
                      new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          String headers = c.sideInput(finalHeadersView);
                          if (!c.element().equals(headers)) {
                            LOG.error("Headers do not match, consistency cannot be guaranteed");
                            throw new RuntimeException(
                                "Headers do not match, consistency cannot be guaranteed");
                          }
                        }
                      })
                  .withSideInputs(finalHeadersView));

      return lineFailsafeElements.apply(
          "LineToDocumentWithHeaders",
          ParDo.of(
                  new FailsafeElementToJsonFn(
                      headersView, jsonSchemaPath(), delimiter(), udfDeadletterTag()))
              .withSideInputs(headersView)
              .withOutputTags(udfOutputTag(), TupleTagList.of(udfDeadletterTag())));
    }

    /** Builder for {@link LineToFailsafeJson}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDelimiter(String delimiter);

      public abstract Builder setUdfFileSystemPath(String udfFileSystemPath);

      public abstract Builder setUdfFunctionName(String udfFunctionName);

      public abstract Builder setUdfReloadIntervalMinutes(int udfReloadIntervalMinutes);

      public abstract Builder setJsonSchemaPath(String jsonSchemaPath);

      public abstract Builder setJsonSchema(String jsonSchema);

      public abstract Builder setHeaderTag(TupleTag<String> headerTag);

      public abstract Builder setLineTag(TupleTag<String> lineTag);

      public abstract Builder setUdfOutputTag(
          TupleTag<FailsafeElement<String, String>> udfOutputTag);

      public abstract Builder setUdfDeadletterTag(
          TupleTag<FailsafeElement<String, String>> udfDeadletterTag);

      public abstract LineToFailsafeJson build();
    }
  }

  /**
   * The {@link FailsafeElementToJsonFn} class creates a Json string from a failsafe element.
   *
   * <p>{@link FailsafeElementToJsonFn#FailsafeElementToJsonFn(PCollectionView, String, String,
   * TupleTag)}
   */
  public static class FailsafeElementToJsonFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {

    @Nullable public final String jsonSchema;
    public final String delimiter;
    public final TupleTag<FailsafeElement<String, String>> udfDeadletterTag;
    @Nullable private final PCollectionView<String> headersView;
    private Counter successCounter =
        Metrics.counter(FailsafeElementToJsonFn.class, SUCCESSFUL_TO_JSON_COUNTER);
    private Counter failedCounter =
        Metrics.counter(FailsafeElementToJsonFn.class, FAILED_TO_JSON_COUNTER);

    FailsafeElementToJsonFn(
        PCollectionView<String> headersView,
        String jsonSchema,
        String delimiter,
        TupleTag<FailsafeElement<String, String>> udfDeadletterTag) {
      this.headersView = headersView;
      this.jsonSchema = jsonSchema;
      this.delimiter = delimiter;
      this.udfDeadletterTag = udfDeadletterTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> element = context.element();
      List<String> header = null;

      if (this.headersView != null) {
        header = Arrays.asList(context.sideInput(this.headersView).split(this.delimiter));
      }

      List<String> record = Arrays.asList(element.getOriginalPayload().split(this.delimiter));

      try {
        String json = buildJsonString(header, record, this.jsonSchema);
        context.output(FailsafeElement.of(element.getOriginalPayload(), json));
        successCounter.inc();
      } catch (Exception e) {
        failedCounter.inc();
        context.output(
            this.udfDeadletterTag,
            FailsafeElement.of(element)
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
      }
    }
  }

  /**
   * The {@link LineToFailsafeElementFn} wraps an csv line with the {@link FailsafeElement} class so
   * errors can be recovered from and the original message can be output to a error records table.
   */
  static class LineToFailsafeElementFn extends DoFn<String, FailsafeElement<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      context.output(FailsafeElement.of(message, message));
    }
  }

  /**
   * The {@link ReadCsv} class is a {@link PTransform} that reads from one for more Csv files. The
   * transform returns a {@link PCollectionTuple} consisting of the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link ReadCsv#headerTag()} - Contains headers found in files if read with headers,
   *       contains empty {@link PCollection} if no headers.
   *   <li>{@link ReadCsv#lineTag()} - Contains Csv lines as a {@link PCollection} of strings.
   * </ul>
   */
  @AutoValue
  public abstract static class ReadCsv extends PTransform<PBegin, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_CsvConverters_ReadCsv.Builder();
    }

    public abstract String csvFormat();

    @Nullable
    public abstract String delimiter();

    public abstract Boolean hasHeaders();

    public abstract String inputFileSpec();

    public abstract TupleTag<String> headerTag();

    public abstract TupleTag<String> lineTag();

    public abstract String fileEncoding();

    @Override
    public PCollectionTuple expand(PBegin input) {

      if (hasHeaders()) {
        return input
            .apply("MatchFilePattern", FileIO.match().filepattern(inputFileSpec()))
            .apply("ReadMatches", FileIO.readMatches())
            .apply(
                "ReadCsvWithHeaders",
                ParDo.of(
                        new GetCsvHeadersFn(
                            headerTag(), lineTag(), csvFormat(), delimiter(), fileEncoding()))
                    .withOutputTags(headerTag(), TupleTagList.of(lineTag())));
      }

      return PCollectionTuple.of(
          lineTag(), input.apply("ReadCsvWithoutHeaders", TextIO.read().from(inputFileSpec())));
    }

    /** Builder for {@link ReadCsv}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setCsvFormat(String csvFormat);

      public abstract Builder setDelimiter(@Nullable String delimiter);

      public abstract Builder setHasHeaders(Boolean hasHeaders);

      public abstract Builder setInputFileSpec(String inputFileSpec);

      public abstract Builder setHeaderTag(TupleTag<String> headerTag);

      public abstract Builder setLineTag(TupleTag<String> lineTag);

      public abstract Builder setFileEncoding(String fileEncoding);

      abstract ReadCsv autoBuild();

      public ReadCsv build() {

        ReadCsv readCsv = autoBuild();

        checkArgument(readCsv.inputFileSpec() != null, "Input file spec must be provided.");

        checkArgument(readCsv.csvFormat() != null, "Csv format must not be null.");

        checkArgument(readCsv.hasHeaders() != null, "Header information must be provided.");

        return readCsv;
      }
    }
  }

  /**
   * The {@link GetCsvHeadersFn} class gets the header of a Csv file and outputs it as a string. The
   * csv format provided in {@link CsvConverters#getCsvFormat(String, String)} is used to get the
   * header.
   */
  static class GetCsvHeadersFn extends DoFn<ReadableFile, String> {

    private final TupleTag<String> headerTag;
    private final TupleTag<String> linesTag;
    private CSVFormat csvFormat;
    private final String fileEncoding;
    private final String delimiter;

    GetCsvHeadersFn(
        TupleTag<String> headerTag,
        TupleTag<String> linesTag,
        String csvFormat,
        String delimiter,
        String fileEncoding) {
      this.headerTag = headerTag;
      this.linesTag = linesTag;
      this.csvFormat = getCsvFormat(csvFormat, delimiter);
      this.fileEncoding = fileEncoding;
      this.delimiter = String.valueOf(this.csvFormat.getDelimiter());
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver outputReceiver) {
      ReadableFile filePath = context.element();
      String headers;
      try {
        BufferedReader bufferedReader =
            new BufferedReader(
                Channels.newReader(filePath.open(), Charset.forName(this.fileEncoding).name()));
        CSVParser parser =
            CSVParser.parse(bufferedReader, this.csvFormat.withFirstRecordAsHeader());
        outputReceiver
            .get(this.headerTag)
            .output(String.join(this.delimiter, parser.getHeaderNames()));
        parser
            .iterator()
            .forEachRemaining(
                record ->
                    outputReceiver.get(this.linesTag).output(String.join(this.delimiter, record)));
      } catch (IOException ioe) {
        LOG.error("Headers do not match, consistency cannot be guaranteed");
        throw new RuntimeException("Could not read Csv headers: " + ioe.getMessage());
      }
    }
  }

  /**
   * The {@link StringToGenericRecordFn} class takes in a String as input and outputs a {@link
   * GenericRecord}.
   */
  public static class StringToGenericRecordFn extends DoFn<String, GenericRecord> {
    private String serializedSchema;
    private final String delimiter;
    private Schema schema;
    private boolean logDetailedCsvConversionErrors = false;

    public StringToGenericRecordFn(String schemaLocation, String delimiter) {
      withSchemaLocation(schemaLocation);
      this.delimiter = delimiter;
    }

    public StringToGenericRecordFn(String delimiter) {
      this.delimiter = delimiter;
    }

    public StringToGenericRecordFn withSchemaLocation(String schemaLocation) {
      this.serializedSchema = GCSUtils.getGcsFileAsString(schemaLocation);
      return this;
    }

    public StringToGenericRecordFn withSerializedSchema(String serializedSchema) {
      this.serializedSchema = serializedSchema;
      return this;
    }

    public StringToGenericRecordFn withLogDetailedCsvConversionErrors(
        boolean logDetailedCsvConversionErrors) {
      this.logDetailedCsvConversionErrors = logDetailedCsvConversionErrors;
      return this;
    }

    @Setup
    public void setup() {
      schema = SchemaUtils.parseAvroSchema(serializedSchema);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IllegalArgumentException {
      GenericRecord genericRecord = new GenericData.Record(schema);
      String[] rowValue =
          Splitter.on(delimiter).splitToList(context.element()).toArray(new String[0]);
      List<Schema.Field> fields = schema.getFields();

      try {
        for (int index = 0; index < fields.size(); ++index) {
          Schema.Field field = fields.get(index);
          String fieldType = field.schema().getType().getName().toLowerCase();

          // Handle null values to be added in generic records, if present in Csv data.
          if (fieldType.equals("union")) {
            String dataType1 = field.schema().getTypes().get(0).getType().getName().toLowerCase();
            String dataType2 = field.schema().getTypes().get(1).getType().getName().toLowerCase();

            // Check if Csv data is null.
            if ((dataType1.equals("null") || dataType2.equals("null"))
                && rowValue[index].length() == 0) {
              genericRecord.put(field.name(), null);
            } else {
              // Add valid data type to generic record.
              if (dataType1.equals("null")) {
                populateGenericRecord(genericRecord, dataType2, rowValue[index], field.name());
              } else {
                populateGenericRecord(genericRecord, dataType1, rowValue[index], field.name());
              }
            }
          } else {
            populateGenericRecord(genericRecord, fieldType, rowValue[index], field.name());
          }
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        LOG.error("Number of fields in the Avro schema and number of Csv headers do not match.");
        throw new RuntimeException(
            "Number of fields in the Avro schema and number of Csv headers do not match.");
      }
      context.output(genericRecord);
    }

    private void populateGenericRecord(
        GenericRecord genericRecord, String fieldType, String data, String fieldName) {

      try {
        switch (fieldType) {
          case "string":
            genericRecord.put(fieldName, data);
            break;
          case "int":
            genericRecord.put(fieldName, Integer.valueOf(data));
            break;
          case "long":
            genericRecord.put(fieldName, Long.valueOf(data));
            break;
          case "float":
            genericRecord.put(fieldName, Float.valueOf(data));
            break;
          case "double":
            genericRecord.put(fieldName, Double.valueOf(data));
            break;
          case "boolean":
            genericRecord.put(fieldName, Boolean.valueOf(data));
            break;
          default:
            LOG.error("{} field type is not supported.", fieldType);
            throw new IllegalArgumentException(fieldType + " field type is not supported.");
        }
      } catch (Exception e) {
        if (logDetailedCsvConversionErrors) {
          String msg =
              String.format(
                  "Failed to convert string '%s' to %s (field name = %s).",
                  data, fieldType, fieldName);
          LOG.error(msg, e);
          throw new RuntimeException(msg, e);
        } else {
          String msg = String.format("Failed to convert field '%s'.", fieldName);
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
      }
    }
  }
}
