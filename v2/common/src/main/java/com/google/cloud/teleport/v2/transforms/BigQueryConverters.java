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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.utils.SerializableSchemaSupplier;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CharMatcher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Teleport BigQueryIO. */
public class BigQueryConverters {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryConverters.class);

  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /** Converts from the BigQuery Avro format into Bigtable mutation. */
  @AutoValue
  public abstract static class AvroToMutation
      implements SerializableFunction<SchemaAndRecord, Mutation> {

    public abstract String columnFamily();

    public abstract String rowkey();

    /** Builder for AvroToEntity. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setColumnFamily(String value);

      public abstract Builder setRowkey(String rowkey);

      public abstract AvroToMutation build();
    }

    public static Builder newBuilder() {
      return new AutoValue_BigQueryConverters_AvroToMutation.Builder();
    }

    public Mutation apply(SchemaAndRecord record) {
      GenericRecord row = record.getRecord();
      String rowkey = row.get(rowkey()).toString();
      Put put = new Put(Bytes.toBytes(rowkey));

      List<TableFieldSchema> columns = record.getTableSchema().getFields();
      for (TableFieldSchema column : columns) {
        String columnName = column.getName();
        if (columnName.equals(rowkey())) {
          continue;
        }

        Object columnObj = row.get(columnName);
        byte[] columnValue = columnObj == null ? null : Bytes.toBytes(columnObj.toString());
        // TODO(billyjacobson): handle other types and column families
        put.addColumn(Bytes.toBytes(columnFamily()), Bytes.toBytes(columnName), columnValue);
      }
      return put;
    }
  }

  /**
   * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
   * RuntimeException} will be thrown.
   *
   * @param json The JSON string to parse.
   * @return The parsed {@link TableRow} object.
   */
  public static TableRow convertJsonToTableRow(String json) {
    TableRow row;
    // Parse the JSON into a {@link TableRow} object.
    try (InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }

    return row;
  }

  /**
   * Creates a {@link Write} transform based on {@code options}.
   *
   * <p>Along with the values in {@code options}, the following are set by default:
   *
   * <ul>
   *   <li>{@link InsertRetryPolicy#retryTransientErrors()}
   *   <li>{@link Write#withExtendedErrorInfo()}
   * </ul>
   *
   * <p>It is the responsibility of the caller to set the schema and write method on the returned
   * value.
   *
   * @param options The options for configuring this write transform.
   * @param <T> The {@link POutput} type of this write. Since type inference does not work when
   *     setting a schema on the returned {@link Write}, this value must be explicitly set.
   * @return The write transform, which can be further configured as needed.
   */
  public static <T> Write<T> createWriteTransform(WriteOptions options) {
    return BigQueryIO.<T>write()
        .to(options.getOutputTableSpec())
        .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
        .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
        .withExtendedErrorInfo();
  }


  public static Write<TableRow> createTableRowWriteTransform(WriteOptions options) {
    return BigQueryIO.writeTableRows()
        .to(new DynamicOutputTable(options.getOutputTableSpec()))
        .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
        .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
        .withExtendedErrorInfo();
  }


  private static class DynamicOutputTable
      implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

      private String outputTableSpec;

      private DynamicOutputTable(String outputTableSpec) {
        this.outputTableSpec = outputTableSpec;
      }
      
      @Override
      public TableDestination apply(ValueInSingleWindow<TableRow> element) {
          TableRow row = element.getValue();
          String dest = formatStringTemplate(outputTableSpec, row);
          return new TableDestination(dest, dest);
      }
  }
 
  /**
   * The {@link TableRowToJsonFn} class converts a tableRow to Json using {@link
   * #tableRowToJson(TableRow)}.
   */
  public static class TableRowToJsonFn extends DoFn<TableRow, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow row = context.element();
      context.output(tableRowToJson(row));
    }
  }

  /** Converts a {@link TableRow} into a Json string using {@link Gson}. */
  public static String tableRowToJson(TableRow row) {
    return new Gson().toJson(row, TableRow.class);
  }

  /**
   * The {@link BigQueryReadOptions} interface contains option necessary to interface with BigQuery.
   */
  public interface BigQueryReadOptions extends PipelineOptions {
    @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
    String getInputTableSpec();

    void setInputTableSpec(String inputTableSpec);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format. If it doesn't exist, it will be created during pipeline execution.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String outputDeadletterTable);

    @Description("Optional: Query to run against input table")
    String getQuery();

    void setQuery(String query);

    @Description("Set to true to use legacy SQL. Default:false")
    @Default.Boolean(false)
    Boolean getUseLegacySql();

    void setUseLegacySql(Boolean useLegacySql);
  }

  /**
   * The {@link FailsafeJsonToTableRow} transform converts JSON strings to {@link TableRow} objects.
   * The transform accepts a {@link FailsafeElement} object so the original payload of the incoming
   * record can be maintained across multiple series of transforms.
   */
  @AutoValue
  public abstract static class FailsafeJsonToTableRow<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_BigQueryConverters_FailsafeJsonToTableRow.Builder<>();
    }

    public abstract TupleTag<TableRow> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> failsafeElements) {
      return failsafeElements.apply(
          "JsonToTableRow",
          ParDo.of(
                  new DoFn<FailsafeElement<T, String>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      FailsafeElement<T, String> element = context.element();
                      String json = element.getPayload();

                      try {
                        TableRow row = convertJsonToTableRow(json);
                        context.output(row);
                      } catch (Exception e) {
                        context.output(
                            failureTag(),
                            FailsafeElement.of(element)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                      }
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    /** Builder for {@link FailsafeJsonToTableRow}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setSuccessTag(TupleTag<TableRow> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafeJsonToTableRow<T> build();
    }
  }

  /**
   * The {@link ReadBigQuery} class reads from BigQuery using {@link BigQueryIO}. The transform
   * returns a {@link PCollection} of {@link TableRow}.
   */
  @AutoValue
  public abstract static class ReadBigQuery extends PTransform<PBegin, PCollection<TableRow>> {

    public static Builder newBuilder() {
      return new AutoValue_BigQueryConverters_ReadBigQuery.Builder();
    }

    public abstract BigQueryReadOptions options();

    @Override
    public PCollection<TableRow> expand(PBegin pipeline) {

      if (options().getQuery() == null) {
        LOG.info("No query provided, reading directly from: " + options().getInputTableSpec());
        return pipeline.apply(
            "ReadFromBigQuery",
            BigQueryIO.readTableRows()
                .from(options().getInputTableSpec())
                .withTemplateCompatibility()
                .withMethod(Method.DIRECT_READ)
                .withCoder(TableRowJsonCoder.of()));

      } else {
        LOG.info("Using query: " + options().getQuery());

        if (!options().getUseLegacySql()) {

          LOG.info("Using Standard SQL");
          return pipeline.apply(
              "ReadFromBigQueryWithQuery",
              BigQueryIO.readTableRows()
                  .fromQuery(options().getQuery())
                  .withTemplateCompatibility()
                  .usingStandardSql()
                  .withCoder(TableRowJsonCoder.of()));
        } else {

          LOG.info("Using Legacy SQL");
          return pipeline.apply(
              "ReadFromBigQueryWithQuery",
              BigQueryIO.readTableRows()
                  .fromQuery(options().getQuery())
                  .withTemplateCompatibility()
                  .withCoder(TableRowJsonCoder.of()));
        }
      }
    }

    /** Builder for {@link ReadBigQuery}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOptions(BigQueryReadOptions options);

      abstract ReadBigQuery autoBuild();

      public ReadBigQuery build() {

        ReadBigQuery readBigQuery = autoBuild();

        if (readBigQuery.options().getInputTableSpec() == null) {
          checkArgument(
              readBigQuery.options().getQuery() != null,
              "If no inputTableSpec is provided then a query is required.");
        }

        if (readBigQuery.options().getQuery() == null) {
          checkArgument(
              readBigQuery.options().getInputTableSpec() != null,
              "If no query is provided then an inputTableSpec is required.");
        }

        return readBigQuery;
      }
    }
  }

  /**
   * The {@link TableRowToFailsafeJsonDocument} class is a {@link PTransform} which transforms
   * {@link TableRow} objects into Json documents for insertion into Elasticsearch. Optionally a
   * javascript UDF can be supplied to parse the {@link TableRow} object. The executions of the UDF
   * and transformation to {@link TableRow} objects is done in a fail-safe way by wrapping the
   * element with it's original payload inside the {@link FailsafeElement} class. The {@link
   * TableRowToFailsafeJsonDocument} transform will output a {@link PCollectionTuple} which contains
   * all output and dead-letter {@link PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link TableRowToFailsafeJsonDocument#transformOutTag()} - Contains all records
   *       successfully converted from JSON to {@link TableRow} objects.
   *   <li>{@link TableRowToFailsafeJsonDocument#transformDeadletterOutTag()} - Contains all {@link
   *       FailsafeElement} records which couldn't be converted to table rows.
   * </ul>
   */
  @AutoValue
  public abstract static class TableRowToFailsafeJsonDocument
      extends PTransform<PCollection<TableRow>, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_BigQueryConverters_TableRowToFailsafeJsonDocument.Builder();
    }

    public abstract JavascriptTextTransformerOptions options();

    public abstract TupleTag<FailsafeElement<TableRow, String>> udfOutTag();

    public abstract TupleTag<FailsafeElement<TableRow, String>> udfDeadletterOutTag();

    public abstract TupleTag<FailsafeElement<TableRow, String>> transformOutTag();

    public abstract TupleTag<FailsafeElement<TableRow, String>> transformDeadletterOutTag();

    @Override
    public PCollectionTuple expand(PCollection<TableRow> input) {

      PCollectionTuple udfOut;

      PCollectionTuple failsafeTableRows =
          input.apply(
              "TableRowToFailsafeElement",
              ParDo.of(new TableRowToFailsafeElementFn(transformDeadletterOutTag()))
                  .withOutputTags(transformOutTag(), TupleTagList.of(transformDeadletterOutTag())));

      // Use Udf to parse table rows if supplied.
      if (options().getJavascriptTextTransformGcsPath() != null) {
        udfOut =
            failsafeTableRows
                .get(transformOutTag())
                .apply(
                    "ProcessFailsafeRowsUdf",
                    JavascriptTextTransformer.FailsafeJavascriptUdf.<TableRow>newBuilder()
                        .setFileSystemPath(options().getJavascriptTextTransformGcsPath())
                        .setFunctionName(options().getJavascriptTextTransformFunctionName())
                        .setSuccessTag(udfOutTag())
                        .setFailureTag(udfDeadletterOutTag())
                        .build());

        PCollection<FailsafeElement<TableRow, String>> failedOut =
            PCollectionList.of(udfOut.get(udfDeadletterOutTag()))
                .and(failsafeTableRows.get(transformDeadletterOutTag()))
                .apply("FlattenFailedOut", Flatten.pCollections());

        return PCollectionTuple.of(transformOutTag(), udfOut.get(udfOutTag()))
            .and(transformDeadletterOutTag(), failedOut);
      } else {
        return failsafeTableRows;
      }
    }

    /** Builder for {@link TableRowToFailsafeJsonDocument}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOptions(JavascriptTextTransformerOptions options);

      public abstract Builder setTransformOutTag(
          TupleTag<FailsafeElement<TableRow, String>> transformOutTag);

      public abstract Builder setTransformDeadletterOutTag(
          TupleTag<FailsafeElement<TableRow, String>> transformDeadletterOutTag);

      public abstract Builder setUdfOutTag(TupleTag<FailsafeElement<TableRow, String>> udfOutTag);

      public abstract Builder setUdfDeadletterOutTag(
          TupleTag<FailsafeElement<TableRow, String>> udfDeadletterOutTag);

      public abstract TableRowToFailsafeJsonDocument build();
    }
  }

  /**
   * The {@link TableRowToFailsafeElementFn} wraps an {@link TableRow} with the {@link
   * FailsafeElement} class so errors can be recovered from and the original message can be output
   * to a error records table.
   */
  static class TableRowToFailsafeElementFn
      extends DoFn<TableRow, FailsafeElement<TableRow, String>> {

    private final TupleTag<FailsafeElement<TableRow, String>> transformDeadletterOutTag;

    /** {@link Counter} for successfully processed elements. */
    private Counter successCounter =
        Metrics.counter(TableRowToFailsafeElementFn.class, "SuccessProcessCounter");

    /** {@link Counter} for un-successfully processed elements. */
    private Counter failedCounter =
        Metrics.counter(TableRowToFailsafeElementFn.class, "FailedProcessCounter");

    TableRowToFailsafeElementFn(
        TupleTag<FailsafeElement<TableRow, String>> transformDeadletterOutTag) {

      this.transformDeadletterOutTag = transformDeadletterOutTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow row = context.element();
      try {
        context.output(FailsafeElement.of(row, tableRowToJson(row)));
        successCounter.inc();
      } catch (Exception e) {
        context.output(
            this.transformDeadletterOutTag,
            FailsafeElement.of(row, row.toString())
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
        failedCounter.inc();
      }
    }
  }

  /**
   * The {@link FailsafeTableRowToFailsafeStringFn} converts a {@link FailsafeElement} containing a
   * {@link TableRow} and string into a {@link FailsafeElement} containing two strings. The output
   * {@link FailsafeElement#getOriginalPayload()} will return {@link TableRow#toString()}.
   */
  public static class FailsafeTableRowToFailsafeStringFn
      extends DoFn<FailsafeElement<TableRow, String>, FailsafeElement<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<TableRow, String> element = context.element();
      context.output(
          FailsafeElement.of(element.getOriginalPayload().toString(), element.getPayload()));
    }
  }

  /**
   * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
   *
   * @param insertError BigQueryInsert error.
   * @return FailsafeElement object.
   * @throws IOException
   */
  public static FailsafeElement<String, String> wrapBigQueryInsertError(
      BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      String rowPayload = JSON_FACTORY.toString(insertError.getRow());
      String errorMessage = JSON_FACTORY.toString(insertError.getError());

      failsafeElement = FailsafeElement.of(rowPayload, rowPayload);
      failsafeElement.setErrorMessage(errorMessage);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return failsafeElement;
  }

  /**
   * Returns {@code String} using Key/Value style formatting.
   *
   * <p>Extracts TableRow fields and applies values to the formatTemplate. ie.
   * formatStringTemplate("I am {key}", {"key": "formatted"}) -> "I am formatted"
   *
   * @param formatTemplate a String with bracketed keys to apply "I am a {key}"
   * @param row is a TableRow object which is used to supply key:values to the template
   */
  public static String formatStringTemplate(String formatTemplate, TableRow row) {
    // Key/Value Map used to replace values in template
    Map<String, String> values = new HashMap<>();

    // Put all column/value pairs into key/value map
    Set<String> rowKeys = row.keySet();
    for (String rowKey : rowKeys) {
      // Only String types can be used in comparison
      if (row.get(rowKey) instanceof String) {
        values.put(rowKey, (String) row.get(rowKey));
      }
    }
    // Substitute any templated values in the template
    String result = StringSubstitutor.replace(formatTemplate, values, "{", "}");
    return result;
  }

  /** A {@link SerializableFunction} to convert a {@link TableRow} to a {@link GenericRecord}. */
  public static class TableRowToGenericRecordFn
      implements SerializableFunction<TableRow, GenericRecord> {

    /**
     * Creates a {@link SerializableFunction} that uses a {@link Schema} to translate a {@link
     * TableRow} into a {@link GenericRecord}.
     *
     * @param avroSchema schema to be used for the {@link GenericRecord}
     * @return a {@link GenericRecord} based on the {@link TableRow}
     */
    public static TableRowToGenericRecordFn of(Schema avroSchema) {
      checkNotNull(avroSchema, "avroSchema is required.");
      return new TableRowToGenericRecordFn(avroSchema);
    }

    private final org.apache.beam.sdk.schemas.Schema beamSchema;
    private final Supplier<Schema> avroSchemaSupplier;

    private TableRowToGenericRecordFn(Schema avroSchema) {
      avroSchemaSupplier = Suppliers.memoize(SerializableSchemaSupplier.of(avroSchema));
      beamSchema = AvroUtils.toBeamSchema(avroSchema);
    }

    @Override
    public GenericRecord apply(TableRow tableRow) {
      Row row = BigQueryUtils.toBeamRow(beamSchema, tableRow);
      return AvroUtils.toGenericRecord(row, avroSchemaSupplier.get());
    }
  }

  /**
   * The {@link BigQueryTableConfigManager} POJO Class to manage the BigQuery Output Table
   * configurations. It allows for a full table path or a set of table template params to be
   * supplied interchangeably.
   *
   * <p>Optionally supply projectIdVal, datasetTemplateVal, and tableTemplateVal or the config
   * manager will default to using outputTableSpec.
   */
  public static class BigQueryTableConfigManager {

    public String projectId;
    public String datasetTemplate;
    public String tableTemplate;

    /**
     * Build a {@code BigQueryTableConfigManager} for use in pipelines.
     *
     * @param projectIdVal The Project ID for the GCP BigQuery project.
     * @param datasetTemplateVal The BQ Dataset value or a templated value.
     * @param tableTemplateVal The BQ Table value or a templated value.
     * @param outputTableSpec The full path of a BQ Table ie. `project:dataset.table`
     *     <p>Optionally supply projectIdVal, datasetTemplateVal, and tableTemplateVal or the config
     *     manager will default to using outputTableSpec.
     */
    public BigQueryTableConfigManager(
        String projectIdVal,
        String datasetTemplateVal,
        String tableTemplateVal,
        String outputTableSpec) {
      if (datasetTemplateVal == null || tableTemplateVal == null) {
        // Legacy Config Option
        List<String> tableObjs = Splitter.on(CharMatcher.anyOf(":.")).splitToList(outputTableSpec);

        this.projectId = tableObjs.get(0);
        this.datasetTemplate = tableObjs.get(1);
        this.tableTemplate = tableObjs.get(2);

        // this.projectId = outputTableSpec.split(":", 2)[0];
        // this.datasetTemplate = outputTableSpec.split(":", 2)[1].split("\\.")[0];
        // this.tableTemplate = outputTableSpec.split(":", 2)[1].split("\\.", 2)[1];
      } else {
        this.projectId = projectIdVal;
        this.datasetTemplate = datasetTemplateVal;
        this.tableTemplate = tableTemplateVal;
      }
    }

    public String getProjectId() {
      return this.projectId;
    }

    public String getDatasetTemplate() {
      return this.datasetTemplate;
    }

    public String getTableTemplate() {
      return this.tableTemplate;
    }

    public String getOutputTableSpec() {
      String tableSpec =
          String.format("%s:%s.%s", this.projectId, this.datasetTemplate, this.tableTemplate);
      return tableSpec;
    }
  }

  /**
   * If deadletterTable is available, it is returned as is, otherwise outputTableSpec +
   * defaultDeadLetterTableSuffix is returned instead.
   */
  /**
   * Return a {@code String} table name to be used as a dead letter queue.
   *
   * @param deadletterTable Default dead letter table to use.
   * @param outputTableSpec Name of the BigQuery output table for successful rows.
   * @param defaultDeadLetterTableSuffix An optional suffix off the successful table.
   */
  public static String maybeUseDefaultDeadletterTable(
      String deadletterTable, String outputTableSpec, String defaultDeadLetterTableSuffix) {
    if (deadletterTable == null) {
      return outputTableSpec + defaultDeadLetterTableSuffix;
    } else {
      return deadletterTable;
    }
  }

  public static final Map<String, LegacySQLTypeName> BQ_TYPE_STRINGS =
      new HashMap<String, LegacySQLTypeName>() {
        {
          put("BOOLEAN", LegacySQLTypeName.BOOLEAN);
          put("BYTES", LegacySQLTypeName.BYTES);
          put("DATE", LegacySQLTypeName.DATE);
          put("DATETIME", LegacySQLTypeName.DATETIME);
          put("FLOAT", LegacySQLTypeName.FLOAT);
          put("INTEGER", LegacySQLTypeName.INTEGER);
          put("NUMERIC", LegacySQLTypeName.NUMERIC);
          put("RECORD", LegacySQLTypeName.RECORD);
          put("STRING", LegacySQLTypeName.STRING);
          put("TIME", LegacySQLTypeName.TIME);
          put("TIMESTAMP", LegacySQLTypeName.TIMESTAMP);
        }
      };

  /**
   * The {@link SchemaUtils} Class to easily convert from a json string to a BigQuery List<Field>.
   */
  public static class SchemaUtils {

    private static final Type gsonSchemaType = new TypeToken<List<Map>>() {}.getType();

    private static Field mapToField(Map fMap) {
      String typeStr = fMap.get("type").toString();
      String nameStr = fMap.get("name").toString();
      String modeStr = fMap.get("mode").toString();
      LegacySQLTypeName type = BQ_TYPE_STRINGS.get(typeStr);
      if (type == null) {
        type = LegacySQLTypeName.STRING;
      }

      return Field.newBuilder(nameStr, type).setMode(Field.Mode.valueOf(modeStr)).build();
    }

    private static List<Field> listToFields(List<Map> jsonFields) {
      List<Field> fields = new ArrayList(jsonFields.size());
      for (Map m : jsonFields) {
        fields.add(mapToField(m));
      }

      return fields;
    }

    /**
     * Return a {@code List<Field>} extracted from a json string.
     *
     * @param schemaStr JSON String with BigQuery schema fields.
     */
    public static List<Field> schemaFromString(String schemaStr) {
      if (schemaStr == null) {
        return null;
      } else {
        Gson gson = new Gson();
        List<Map> jsonFields = gson.fromJson(schemaStr, gsonSchemaType);
        return listToFields(jsonFields);
      }
    }
  }

  /** Converts a row to tableRow via {@link BigQueryUtils#toTableRow()}. */
  public static SerializableFunction<Row, TableRow> rowToTableRowFn = BigQueryUtils::toTableRow;

  /**
   * The {@link FailsafeRowToTableRow} transform converts {@link Row} to {@link TableRow} objects.
   * The transform accepts a {@link FailsafeElement} object so the original payload of the incoming
   * record can be maintained across multiple series of transforms.
   */
  @AutoValue
  public abstract static class FailsafeRowToTableRow<T>
      extends PTransform<PCollection<FailsafeElement<T, Row>>, PCollectionTuple> {

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_BigQueryConverters_FailsafeRowToTableRow.Builder<>();
    }

    public abstract TupleTag<TableRow> successTag();

    public abstract TupleTag<FailsafeElement<T, Row>> failureTag();

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, Row>> failsafeElements) {
      return failsafeElements.apply(
          "FailsafeRowToTableRow",
          ParDo.of(
                  new DoFn<FailsafeElement<T, Row>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      FailsafeElement<T, Row> element = context.element();
                      Row row = element.getPayload();

                      try {
                        TableRow tableRow = BigQueryUtils.toTableRow(row);
                        context.output(tableRow);
                      } catch (Exception e) {
                        context.output(
                            failureTag(),
                            FailsafeElement.of(element)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                      }
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    /** Builder for {@link FailsafeRowToTableRow}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setSuccessTag(TupleTag<TableRow> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, Row>> failureTag);

      public abstract FailsafeRowToTableRow<T> build();
    }
  }
}
