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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.BigQueryToParquet.BigQueryToParquetOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQueryToParquet} pipeline exports data from a BigQuery table to Parquet file(s) in a
 * Google Cloud Storage bucket.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_BigQuery_to_Parquet.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "BigQuery_to_Parquet",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery export to Parquet (via Storage API)",
    description =
        "The BigQuery export to Parquet template is a batch pipeline that reads data from a BigQuery table and writes it to a Cloud Storage bucket in Parquet format. "
            + "This template utilizes the <a href=\"https://cloud.google.com/bigquery/docs/reference/storage\">BigQuery Storage API</a> to export the data.",
    optionsClass = BigQueryToParquetOptions.class,
    flexContainerName = "googlecloud-to-googlecloud",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-parquet",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input BigQuery table must exist before running the pipeline.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class BigQueryToParquet {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToParquet.class);

  /** File suffix for file to be written. */
  private static final String FILE_SUFFIX = ".parquet";

  /**
   * Aligns {@link GenericRecord} instances returned by BigQuery Storage Read API to the target Avro
   * {@link Schema} constructed from {@link BigQueryUtils#toGenericAvroSchema(TableSchema, Boolean)}
   * so that record names/namespaces (for nullable and repeated structs) and field ordering match
   * {@link AvroCoder} and {@link ParquetIO}.
   */
  @VisibleForTesting
  static class RecordSchemaAligner implements SerializableFunction<SchemaAndRecord, GenericRecord> {
    private final String targetSchemaJson;
    private transient volatile Schema targetSchema;

    RecordSchemaAligner(Schema targetSchema) {
      this.targetSchemaJson = targetSchema.toString();
      this.targetSchema = targetSchema;
    }

    private Schema getTargetSchema() {
      Schema schema = targetSchema;
      if (schema == null) {
        schema = new Schema.Parser().parse(targetSchemaJson);
        targetSchema = schema;
      }
      return schema;
    }

    @Override
    public GenericRecord apply(SchemaAndRecord schemaAndRecord) {
      GenericRecord source = schemaAndRecord.getRecord();
      if (source == null) {
        return null;
      }
      return alignRecord(source, getTargetSchema());
    }

    private static GenericRecord alignRecord(GenericRecord source, Schema targetSchema) {
      GenericRecord target = new GenericData.Record(targetSchema);
      for (Schema.Field field : targetSchema.getFields()) {
        Object value = source.get(field.name());
        target.put(field.pos(), alignValue(value, field.schema()));
      }
      return target;
    }

    private static Object alignValue(Object value, Schema targetSchema) {
      if (value == null) {
        return null;
      }
      switch (targetSchema.getType()) {
        case UNION:
          for (Schema branch : targetSchema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
              return alignValue(value, branch);
            }
          }
          return null;
        case RECORD:
          if (value instanceof GenericRecord) {
            return alignRecord((GenericRecord) value, targetSchema);
          }
          return value;
        case ARRAY:
          if (value instanceof Collection) {
            Schema elementSchema = targetSchema.getElementType();
            if (containsRecord(elementSchema)) {
              Collection<?> sourceCollection = (Collection<?>) value;
              GenericData.Array<Object> aligned =
                  new GenericData.Array<>(sourceCollection.size(), targetSchema);
              for (Object element : sourceCollection) {
                aligned.add(alignValue(element, elementSchema));
              }
              return aligned;
            }
          }
          return value;
        default:
          return value;
      }
    }

    private static boolean containsRecord(Schema schema) {
      switch (schema.getType()) {
        case RECORD:
          return true;
        case ARRAY:
          return containsRecord(schema.getElementType());
        case UNION:
          for (Schema branch : schema.getTypes()) {
            if (containsRecord(branch)) {
              return true;
            }
          }
          return false;
        default:
          return false;
      }
    }
  }

  /**
   * The {@link BigQueryToParquetOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToParquetOptions extends PipelineOptions {
    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery table to export",
        groupName = "Source",
        helpText = "The BigQuery input table location.",
        example = "your-project:your-dataset.your-table-name")
    @Required
    String getTableRef();

    void setTableRef(String tableRef);

    @TemplateParameter.GcsWriteFile(
        order = 2,
        description = "Output Cloud Storage file(s)",
        groupName = "Target",
        helpText = "The Cloud Storage folder to write the Parquet files to.",
        example = "gs://your-bucket/export/")
    @Required
    String getBucket();

    void setBucket(String bucket);

    @TemplateParameter.Integer(
        order = 3,
        optional = true,
        description = "Maximum output shards",
        helpText = "The number of output file shards. The default value is `1`.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "List of field names",
        helpText = "A comma-separated list of fields to select from the input BigQuery table.")
    String getFields();

    void setFields(String fields);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Row restrictions/filter.",
        helpText =
            "Read only rows which match the specified filter, which must be a SQL expression"
                + " compatible with Google standard SQL"
                + " (https://cloud.google.com/bigquery/docs/reference/standard-sql). If no value is"
                + " specified, then all rows are returned.")
    String getRowRestriction();

    void setRowRestriction(String restriction);
  }

  /**
   * Parses the comma-separated {@code fields} option into a list of trimmed field names.
   *
   * @param fields Comma-separated list of field names, or null/empty.
   * @return List of selected field names, or an empty list if none are specified.
   */
  @VisibleForTesting
  static List<String> parseSelectedFields(String fields) {
    if (Strings.isNullOrEmpty(fields)) {
      return Collections.emptyList();
    }
    return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(fields);
  }

  /**
   * Trims a BigQuery {@link TableSchema} to include only the specified {@code selectedFields},
   * supporting nested dot-delimited field paths (e.g., {@code struct_col.sub_field}) and
   * case-insensitive BigQuery column name matching.
   *
   * @param schema The full BigQuery {@link TableSchema}.
   * @param selectedFields List of selected field names to retain.
   * @return Trimmed {@link TableSchema} containing only the selected fields.
   */
  @VisibleForTesting
  static TableSchema trimSchema(TableSchema schema, List<String> selectedFields) {
    if (selectedFields == null || selectedFields.isEmpty()) {
      return schema;
    }

    for (String selectedField : selectedFields) {
      if (!hasFieldPath(schema.getFields(), selectedField)) {
        throw new IllegalArgumentException(
            String.format("Selected field '%s' not found in table schema.", selectedField));
      }
    }

    List<TableFieldSchema> trimmedFields =
        schema.getFields().stream()
            .flatMap(f -> trimField(f, selectedFields))
            .collect(Collectors.toList());
    return new TableSchema().setFields(trimmedFields);
  }

  private static boolean hasFieldPath(List<TableFieldSchema> fields, String fieldPath) {
    if (fields == null || Strings.isNullOrEmpty(fieldPath)) {
      return false;
    }
    int dotIndex = fieldPath.indexOf('.');
    if (dotIndex < 0) {
      for (TableFieldSchema field : fields) {
        if (field.getName().equalsIgnoreCase(fieldPath)) {
          return true;
        }
      }
      return false;
    }
    String parentName = fieldPath.substring(0, dotIndex);
    String childPath = fieldPath.substring(dotIndex + 1);
    if (parentName.isEmpty() || childPath.isEmpty()) {
      return false;
    }
    for (TableFieldSchema field : fields) {
      if (field.getName().equalsIgnoreCase(parentName)) {
        return hasFieldPath(field.getFields(), childPath);
      }
    }
    return false;
  }

  private static Stream<TableFieldSchema> trimField(
      TableFieldSchema field, List<String> selectedFields) {
    String name = field.getName();
    if (selectedFields.stream().anyMatch(sf -> sf.equalsIgnoreCase(name))) {
      return Stream.of(field);
    }

    if (field.getFields() != null) {
      List<String> selectedChildren =
          selectedFields.stream()
              .filter(
                  sf ->
                      sf.length() > name.length() + 1
                          && sf.regionMatches(true, 0, name, 0, name.length())
                          && sf.charAt(name.length()) == '.')
              .map(sf -> sf.substring(name.length() + 1))
              .collect(Collectors.toList());

      if (!selectedChildren.isEmpty()) {
        List<TableFieldSchema> trimmedChildren =
            field.getFields().stream()
                .flatMap(c -> trimField(c, selectedChildren))
                .collect(Collectors.toList());

        if (!trimmedChildren.isEmpty()) {
          TableFieldSchema trimmedField = new TableFieldSchema();
          for (Map.Entry<String, Object> entry : field.entrySet()) {
            if (!"fields".equals(entry.getKey())) {
              trimmedField.set(entry.getKey(), entry.getValue());
            }
          }
          trimmedField.setFields(trimmedChildren);
          return Stream.of(trimmedField);
        }
      }
    }

    return Stream.empty();
  }

  /**
   * Fetches the BigQuery {@link TableSchema} via Beam's {@link BigQueryServices}, optionally trims
   * it to {@code selectedFields}, and converts it to an Avro {@link Schema}.
   *
   * @param bigQueryServices BigQuery services used to fetch table metadata.
   * @param options Pipeline options containing the table reference and project settings.
   * @param selectedFields Optional list of fields to filter the schema on.
   * @return Avro schema for the table export.
   */
  @VisibleForTesting
  static Schema getTableSchema(
      BigQueryServices bigQueryServices,
      BigQueryToParquetOptions options,
      List<String> selectedFields) {
    TableReference tableReference = BigQueryHelpers.parseTableSpec(options.getTableRef());
    BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      String defaultProject =
          bigQueryOptions.getBigQueryProject() == null
              ? bigQueryOptions.getProject()
              : bigQueryOptions.getBigQueryProject();
      checkState(
          !Strings.isNullOrEmpty(defaultProject),
          "No project ID set in %s or %s, cannot construct a complete %s",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReference.class.getSimpleName());
      LOG.info(
          "Project ID not set in {}. Using default project from {}.",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName());
      tableReference.setProjectId(defaultProject);
    }

    Table table;
    try (DatasetService datasetService = bigQueryServices.getDatasetService(bigQueryOptions)) {
      table = datasetService.getTable(tableReference);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOG.error(
          "Error fetching BigQuery table metadata for {}: {}",
          options.getTableRef(),
          e.getMessage());
      throw new RuntimeException(
          "Error fetching BigQuery table metadata for " + options.getTableRef(), e);
    }

    if (table == null) {
      throw new IllegalArgumentException(
          String.format("Table %s not found.", BigQueryHelpers.toTableSpec(tableReference)));
    }
    TableSchema tableSchema = table.getSchema();
    if (tableSchema == null || tableSchema.getFields() == null) {
      throw new IllegalArgumentException(
          String.format("Table %s has no schema.", BigQueryHelpers.toTableSpec(tableReference)));
    }

    if (selectedFields != null && !selectedFields.isEmpty()) {
      tableSchema = trimSchema(tableSchema, selectedFields);
    }

    Schema avroSchema = BigQueryUtils.toGenericAvroSchema(tableSchema, true);
    LOG.info("Schema for export is: {}", avroSchema);
    return avroSchema;
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    BigQueryToParquetOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToParquetOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToParquetOptions options) {
    return run(options, null);
  }

  @VisibleForTesting
  static PipelineResult run(
      BigQueryToParquetOptions options, BigQueryServices testBigQueryServices) {
    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    List<String> selectedFields = parseSelectedFields(options.getFields());

    BigQueryServices bigQueryServices =
        testBigQueryServices != null ? testBigQueryServices : new BigQueryServicesImpl();

    // Extract Avro schema from BigQuery table metadata via Beam's BigQueryServices/BigQueryUtils.
    Schema schema = getTableSchema(bigQueryServices, options, selectedFields);

    TypedRead<GenericRecord> readFromBQ =
        BigQueryIO.read(new RecordSchemaAligner(schema))
            .from(options.getTableRef())
            .withTemplateCompatibility()
            .withMethod(Method.DIRECT_READ)
            .withCoder(AvroCoder.of(schema));

    if (testBigQueryServices != null) {
      readFromBQ = readFromBQ.withTestServices(testBigQueryServices);
    }

    if (!selectedFields.isEmpty()) {
      readFromBQ = readFromBQ.withSelectedFields(selectedFields);
    }

    // Add row restrictions/filter if any.
    if (!Strings.isNullOrEmpty(options.getRowRestriction())) {
      readFromBQ = readFromBQ.withRowRestriction(options.getRowRestriction());
    }

    /*
     * Steps: 1) Read records from BigQuery via BigQueryIO.
     *        2) Write records to Google Cloud Storage in Parquet format.
     */
    pipeline
        /*
         * Step 1: Read records via BigQueryIO using supplied schema as a PCollection of
         *         {@link GenericRecord}.
         */
        .apply("ReadFromBigQuery", readFromBQ)
        /*
         * Step 2: Write records to Google Cloud Storage as one or more Parquet files
         *         via {@link ParquetIO}.
         */
        .apply(
            "WriteToParquet",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(schema))
                .to(options.getBucket())
                .withNumShards(options.getNumShards())
                .withSuffix(FILE_SUFFIX));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
