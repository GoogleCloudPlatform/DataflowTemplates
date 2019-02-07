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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests CSV and other type of delimited data from GCS and writes data to a Cloud
 * Spanner database table. Each row from the input CSV file will be applied to Spanner with an
 * InsertOrUpdate mutation, so this can be used both to populate new rows or to update columns of
 * existing rows.
 *
 * <p>You can specify column delimiter other than comma. Also make sure to use field qualifier such
 * as double quote to escape delimiter if it is in the value.
 *
 * <p>Text file must NOT have a header.
 *
 * <p>Example Usage: Here is CSV sample data simulating an account table:
 * 1,sample_user_1,true,2018-01-01,2018-01-01T12:30:00Z
 *
 * <p>Schema file must have all column and type definition in one line. Schema file must use the
 * data type names of Cloud Spanner. We currently support the following Cloud Spanner data types: -
 * BOOL - DATE - FLOAT64 - INT64 - STRING - TIMESTAMP
 *
 * <p>Input format properties: - \\N in the source column will be considered as NULL value when
 * writing to Cloud Spanner. - If you need to escape characters, you can use the "fieldQualifier"
 * parameter to tell the pipeline. e.g. You can put all values inside double quotes like "123",
 * "john", "true" - See the implementation of parseRow() below to see what values are accepted for
 * each data type.
 *
 * <p>NOTE: BYTES, ARRAY, STRUCT types are not supported.
 *
 * <p>Example schema file for the CSV file above:
 *
 * <pre>Id:INT64,Username:STRING,Active:BOOL,CreateDate:DATE,ModifyTime:TIMESTAMP</pre>
 *
 * <p>Here is the DDL for creating Cloud Spanner table:
 *
 * <pre>CREATE TABLE example_table
 * ( Id INT64, Username STRING(MAX), Active BOOL, CreateDate DATE, ModifyTime TIMESTAMP )
 *  PRIMARY KEY(Id)
 * </pre>
 *
 * <pre>
 * {@code mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.TextToSpanner \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://${INPUT_FILE_PATTERN} \
 * --schemaFilename=gs://SCHEMA_FILE_NAME \
 * --spannerInstance=SPANNER_INSTANCE_NAME \
 * --databaseName=DATABASE_NAME \
 * --tableName=TABLE_NAME \
 * --columnDelimiter=',' \
 * --fieldQualifier='"'
 * }
 * </pre>
 */
public class TextToSpanner {

  /** Options supported by {@link TextToSpanner}. */
  public interface Options extends PipelineOptions {
    @Description("GCP Project Id of where the Spanner table lives.")
    ValueProvider<String> getProjectId();

    void setProjectId(ValueProvider<String> value);

    @Description("Input file or filepattern to read from")
    @Validation.Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("Schema file to read from")
    @Validation.Required
    ValueProvider<String> getSchemaFilename();

    void setSchemaFilename(ValueProvider<String> value);

    @Description("Cloud Spanner instance")
    @Validation.Required
    ValueProvider<String> getSpannerInstance();

    void setSpannerInstance(ValueProvider<String> value);

    @Description("Cloud Spanner database name")
    @Validation.Required
    ValueProvider<String> getDatabaseName();

    void setDatabaseName(ValueProvider<String> value);

    @Description("Cloud Spanner table name")
    @Validation.Required
    ValueProvider<String> getTableName();

    void setTableName(ValueProvider<String> value);

    @Description("Column delimiter of the source file")
    @Validation.Required
    @Default.Character(',')
    ValueProvider<Character> getColumnDelimiter();

    void setColumnDelimiter(ValueProvider<Character> value);

    @Description(
        "Field qualifier used by the source file, default is double quote. Field "
            + "qualifier should be used when character needs to be escaped.")
    ValueProvider<Character> getFieldQualifier();

    void setFieldQualifier(ValueProvider<Character> value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TextToSpanner.class);
  private static final ImmutableSet<String> SUPPORTED_DATA_TYPES =
      ImmutableSet.of("INT64", "FLOAT64", "BOOL", "DATE", "TIMESTAMP", "STRING");

  public static Set<String> getSupportDataTypes() {
    return SUPPORTED_DATA_TYPES;
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    Pipeline p = Pipeline.create(options);
    Options textToSpannerOptions = options.as(Options.class);
    RowToMutationFn rowToMutationFn =
        new RowToMutationFn(
            textToSpannerOptions.getProjectId(),
            textToSpannerOptions.getSpannerInstance(),
            textToSpannerOptions.getDatabaseName(),
            textToSpannerOptions.getTableName(),
            textToSpannerOptions.getSchemaFilename(),
            textToSpannerOptions.getColumnDelimiter(),
            textToSpannerOptions.getFieldQualifier());

    p.apply(TextIO.read().from(options.getInputFilePattern()))
        .apply(ParDo.of(rowToMutationFn))
        .apply(
            SpannerIO.write()
                .withInstanceId(options.getSpannerInstance())
                .withDatabaseId(options.getDatabaseName()));

    return p.run();
  }

  /** Cloud Spanner transformation helper that convert each row to Mutation object. */
  static class RowToMutationFn extends DoFn<String, Mutation> {
    private final ValueProvider<String> projectId;
    private final ValueProvider<Character> columnDelimiter;
    private final ValueProvider<Character> fieldQualifier;
    private final ValueProvider<String> spannerTableName;
    private final ValueProvider<String> spannerDBName;
    private final ValueProvider<String> spannerInstanceName;
    private final ValueProvider<String> schemaFileName;

    private SpannerSchema spannerSchema = null;
    private List<String> spannerSchemaColumns = null;
    private Mutation.WriteBuilder writeBuilder = null;
    private CSVFormat csvFormat = null;

    public RowToMutationFn(
        ValueProvider<String> projectId,
        ValueProvider<String> spannerInstanceName,
        ValueProvider<String> spannerDBName,
        ValueProvider<String> spannerTableName,
        ValueProvider<String> schemaFileName,
        ValueProvider<Character> columnDelimiter,
        ValueProvider<Character> fieldQualifier) {
      this.columnDelimiter = columnDelimiter;
      this.spannerTableName = spannerTableName;
      this.spannerDBName = spannerDBName;
      this.spannerInstanceName = spannerInstanceName;
      this.fieldQualifier = fieldQualifier;
      this.projectId = projectId;
      this.schemaFileName = schemaFileName;
    }

    @Setup
    public void setup() {
      SpannerSchema spannerSchemaFromFile = getSpannerSchemaFromFile(schemaFileName);
      SpannerSchema spannerSchemaFromCloud =
          getSpannerSchemaFromSpanner(
              spannerInstanceName.get(), spannerDBName.get(), spannerTableName.get());
      // Validate if schema from the 2 places are compatible
      if (!spannerSchemaFromFile.compatibleWithSchema(spannerSchemaFromCloud)) {
        throw new RuntimeException(
            "Schema from file and schema from Cloud Spanner table is not compatible!");
      }
      this.spannerSchema = spannerSchemaFromFile;
      this.spannerSchemaColumns = spannerSchemaFromFile.getColumnList();

      // Initialize CSV Parser format
      csvFormat =
          CSVFormat.newFormat(columnDelimiter.get())
              .withEscape('\\')
              .withIgnoreEmptyLines(false)
              .withNullString("\\N");
      if (fieldQualifier != null) {
        csvFormat = csvFormat.withQuote(fieldQualifier.get());
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      /**
       * Input string is one line but Apache CSVParser process multiple line, so we only take the
       * first item in the result list
       */
      Reader in = new StringReader(c.element());
      CSVParser parser = new CSVParser(in, csvFormat);
      List<CSVRecord> list = parser.getRecords();
      if (list.isEmpty()) {
        return;
      }
      if (list.size() > 1) {
        throw new RuntimeException("Unable to parse this row: " + c.element());
      }
      CSVRecord row = list.get(0);

      writeBuilder = Mutation.newInsertOrUpdateBuilder(spannerTableName.get());
      c.output(parseRow(writeBuilder, row, spannerSchema));
    }

    /**
     * Take a String list and convert into a Mutation object base on schema provided.
     *
     * @param builder MutationBuilder to construct
     * @param row CSVRecord parsed list of data cell
     * @param spannerSchema list with column name and column data type
     * @return build Mutation object for the row
     */
    protected final Mutation parseRow(
        Mutation.WriteBuilder builder, CSVRecord row, SpannerSchema spannerSchema)
        throws IllegalArgumentException {
      if (row.size() != spannerSchemaColumns.size()) {
        throw new RuntimeException(
            "Parsed row has different number of column than schema. "
                + "Row content: "
                + row.toString());
      }

      // Extract cell by cell and construct Mutation object
      for (int i = 0; i < row.size(); i++) {
        String columnName = spannerSchemaColumns.get(i);
        SpannerDataTypes columnType = spannerSchema.getColumnType(columnName);
        String cellValue = row.get(i);

        if (cellValue == null) {
          builder.set(columnName).to(Value.string(null));
        } else {
          // TODO: make the tests below match Spanner's SQL literal rules wherever possible,
          // in terms of how input is accepted, and throw exceptions on invalid input.
          switch (columnType) {
            case BOOL:
              Boolean bCellValue;
              if (cellValue.trim().equalsIgnoreCase("true")) {
                bCellValue = Boolean.TRUE;
              } else if (cellValue.trim().equalsIgnoreCase("false")) {
                bCellValue = Boolean.FALSE;
              } else {
                throw new IllegalArgumentException(
                    cellValue.trim() + " is not recognizable value " + "for BOOL type");
              }
              builder.set(columnName).to(Boolean.valueOf(cellValue));
              break;
            case INT64:
              builder.set(columnName).to(Integer.valueOf(cellValue.trim()));
              break;
            case STRING:
              builder.set(columnName).to(cellValue.trim());
              break;
            case FLOAT64:
              builder.set(columnName).to(Float.valueOf(cellValue.trim()));
              break;
            case DATE:
              // This requires date type in format of 'YYYY-[M]M-[D]D'
              builder.set(columnName).to(com.google.cloud.Date.parseDate(cellValue.trim()));
              break;
            case TIMESTAMP:
              builder
                  .set(columnName)
                  .to(
                      com.google.cloud.Timestamp.parseTimestamp(
                          cellValue.replaceAll("\"", "").trim()));
              break;
            default:
              throw new IllegalArgumentException(
                  "Unrecognized column data type: " + spannerSchema.getColumnType(columnName));
          }
        }
      }

      return builder.build();
    }

    /*
     * Query Cloud Spanner to get the table's schema
     */
    public SpannerSchema getSpannerSchemaFromSpanner(
        String spannerInstanceName, String spannerDatabaseName, String spannerTableName) {
      String schemaQuery =
          "SELECT column_name,spanner_type FROM "
              + "information_schema.columns WHERE table_catalog = '' AND table_schema = '' "
              + "AND table_name = @tableName";

      SpannerSchema spannerSchema = new SpannerSchema();
      SpannerOptions options =
          SpannerOptions.newBuilder().setProjectId(this.projectId.get()).build();
      Spanner spanner = options.getService();

      try {
        DatabaseId db =
            DatabaseId.of(options.getProjectId(), spannerInstanceName, spannerDatabaseName);
        DatabaseClient dbClient = spanner.getDatabaseClient(db);
        ResultSet resultSet =
            dbClient
                .singleUse()
                .executeQuery(
                    Statement.newBuilder(schemaQuery)
                        .bind("tableName")
                        .to(spannerTableName)
                        .build());
        if (resultSet == null) {
          throw new RuntimeException("Could not get result of Cloud Spanner table schema!");
        }
        while (resultSet.next()) {
          String columnName = resultSet.getString(0);
          String columnType = resultSet.getString(1);

          if (Strings.isNullOrEmpty(columnName) || Strings.isNullOrEmpty(columnType)) {
            throw new RuntimeException("Could not find valid column name or type. They are null.");
          }
          spannerSchema.addEntry(columnName, SpannerSchema.parseSpannerDataType(columnType));
        }
      } finally {
        spanner.close();
      }

      LOG.info("[GetSpannerSchema] Closed Cloud Spanner DB client");
      return spannerSchema;
    }

    /**
     * Helper method to read schema file and convert content to SpannerSchema object.
     *
     * @param schemaFilename GCS location of the schema file
     * @return parsed SpannerSchema object
     */
    public SpannerSchema getSpannerSchemaFromFile(ValueProvider<String> schemaFilename) {
      if (schemaFilename == null) {
        throw new RuntimeException("No schema file provided!");
      }
      try {
        ReadableByteChannel readableByteChannel =
            FileSystems.open(FileSystems.matchNewResource(schemaFilename.get(), false));
        String schemaString =
            new String(
                StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)));

        SpannerSchema spannerSchema = new SpannerSchema(schemaString);

        return spannerSchema;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

/*
 * This enum represents the Cloud Spanner data types that supported by this Dataflow pipeline.
 */
enum SpannerDataTypes {
  INT64,
  FLOAT64,
  BOOL,
  DATE,
  TIMESTAMP,
  STRING
};

/*
 * Represent a Cloud Spanner Schema. The key is the column name and the value is data type
 */
class SpannerSchema {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerSchema.class);
  private static final String SCHEMA_COLUMN_DELIMITER = ",";
  private static final String SCHEMA_DELIMITER = ":";
  Map<String, SpannerDataTypes> spannerSchema = new LinkedHashMap<>();

  public SpannerSchema(String schemaString) {
    for (String pair : schemaString.split(SCHEMA_COLUMN_DELIMITER)) {
      String[] columnPair = pair.split(SCHEMA_DELIMITER);
      spannerSchema.put(columnPair[0], parseSpannerDataType(columnPair[1]));
    }
  }

  public SpannerSchema() {}

  public static SpannerDataTypes parseSpannerDataType(String columnType) {
    if (columnType.matches("STRING(?:\\((?:MAX|[0-9]+)\\))?")) {
      return SpannerDataTypes.STRING;
    } else if (columnType.equalsIgnoreCase("FLOAT64")) {
      return SpannerDataTypes.FLOAT64;
    } else if (columnType.equalsIgnoreCase("INT64")) {
      return SpannerDataTypes.INT64;
    } else if (columnType.equalsIgnoreCase("BOOL")) {
      return SpannerDataTypes.BOOL;
    } else if (columnType.equalsIgnoreCase("DATE")) {
      return SpannerDataTypes.DATE;
    } else if (columnType.equalsIgnoreCase("TIMESTAMP")) {
      return SpannerDataTypes.TIMESTAMP;
    } else {
      throw new IllegalArgumentException(
          "Unrecognized or unsupported column data type: " + columnType);
    }
  }

  public void addEntry(String columnName, SpannerDataTypes columnType) {
    spannerSchema.put(columnName, columnType);
  }

  public void addEntry(String columnName, String columnType) {
    spannerSchema.put(columnName, parseSpannerDataType(columnType));
  }

  public SpannerDataTypes getColumnType(String key) {
    return this.spannerSchema.get(key);
  }

  public List<String> getColumnList() {
    List<String> result = new ArrayList<>();
    result.addAll(spannerSchema.keySet());

    return result;
  }

  public boolean compatibleWithSchema(SpannerSchema targetSpannerSchema) {
    boolean result = true;
    for (String columnName : spannerSchema.keySet()) {
      if (!targetSpannerSchema.spannerSchema.containsKey(columnName)) {
        LOG.info("Schema does not contain column: " + columnName);
        result = false;
      }

      if (getColumnType(columnName) != targetSpannerSchema.getColumnType(columnName)) {
        LOG.info("In schema, column: " + columnName + " does not has the same data type.");
        LOG.info(
            "Source type:"
                + getColumnType(columnName).toString()
                + ", the other type:"
                + targetSpannerSchema.getColumnType(columnName).toString());
        result = false;
      }
    }
    return result;
  }
}
