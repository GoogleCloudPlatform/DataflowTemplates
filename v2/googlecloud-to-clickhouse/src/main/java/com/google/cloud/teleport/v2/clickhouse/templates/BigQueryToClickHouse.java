/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse.templates;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.clickhouse.options.BigQueryToClickHouseOptions;
import com.google.cloud.teleport.v2.clickhouse.utils.ClickHouseUtils;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.io.clickhouse.TableSchema;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "BigQuery_to_ClickHouse",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to ClickHouse",
    description =
        "The BigQuery to ClickHouse template is a batch pipeline that ingests data from a BigQuery table into ClickHouse table. "
            + "The template can either read the entire table or read specific records using a supplied query.",
    optionsClass = BigQueryToClickHouseOptions.class,
    skipOptions = {
      "javascriptTextTransformReloadIntervalMinutes",
      "pythonExternalTextTransformGcsPath",
      "pythonExternalTextTransformFunctionName"
    },
    flexContainerName = "bigquery-to-clickhouse",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-clickhouse",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The source BigQuery table must exist.",
      "The ClickHouse target table must exist prior running.",
      "This ClickHouse target table must have the exact same column names as the the source table/query."
    })
public class BigQueryToClickHouse {

  private static final Logger log = LoggerFactory.getLogger(BigQueryToClickHouse.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    BigQueryToClickHouseOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigQueryToClickHouseOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToClickHouseOptions options) {
    try {
      checkArgument(
          !options.getJdbcUrl().isEmpty(),
          "The ClickHouse JDBC url must have the following template: `jdbc:clickhouse://host:port/schema`");
      checkArgument(
          !options.getClickHouseTable().isEmpty(),
          "ClickHouse target table name is empty. Please provide a valid table name.");

      String clickHouseJDBCURL =
          ClickHouseUtils.setJDBCCredentials(
              options.getJdbcUrl(),
              options.getClickHouseUsername(),
              options.getClickHousePassword());
      TableSchema clickHouseSchema =
          ClickHouseIO.getTableSchema(clickHouseJDBCURL, options.getClickHouseTable());
      Schema beamSchema = TableSchema.getEquivalentSchema(clickHouseSchema);

      // Create the pipeline.
      Pipeline pipeline = Pipeline.create(options);

      /*
       * Step #1: Read from BigQuery. If a query is provided then it is used to get the TableRows.
       */
      PCollection<TableRow> tableRows =
          pipeline.apply(
              "Read From Big Query",
              BigQueryConverters.ReadBigQueryTableRows.newBuilder()
                  .setOptions(options.as(BigQueryToClickHouseOptions.class))
                  .build());

      // Step 2: Transform TableRow to Row
      PCollection<Row> rows =
          tableRows
              .apply(
                  "Convert to Beam Row",
                  ParDo.of(new TableRowToBeamRowFn(beamSchema, clickHouseSchema)))
              .setRowSchema(beamSchema);

      ClickHouseIO.Write clickHouseWriter =
          ClickHouseIO.write(clickHouseJDBCURL, options.getClickHouseTable());

      if (options.getMaxInsertBlockSize() != null) {
        clickHouseWriter.withMaxInsertBlockSize(options.getMaxInsertBlockSize());
      }
      if (options.getInsertDistributedSync() != null) {
        clickHouseWriter.withInsertDistributedSync(options.getInsertDistributedSync());
      }
      if (options.getInsertQuorum() != null) {
        clickHouseWriter.withInsertQuorum(options.getInsertQuorum());
      }
      if (options.getInsertDeduplicate() != null) {
        clickHouseWriter.withInsertDeduplicate(options.getInsertDeduplicate());
      }
      if (options.getMaxRetries() != null) {
        clickHouseWriter.withMaxRetries(options.getMaxRetries());
      }

      // Step 3: Write data to ClickHouse
      rows.apply("Write to ClickHouse", clickHouseWriter);

      return pipeline.run();
    } catch (Exception e) {
      log.error("Error occurred during the BigQuery to ClickHouse template execution: ", e);
      throw new RuntimeException(e);
    }
  }
}

class TableRowToBeamRowFn extends DoFn<TableRow, Row> {
  private Map<String, TableSchema.Column> columnMap;
  private final Schema beamSchema;
  private final TableSchema clickHouseSchema;

  public TableRowToBeamRowFn(Schema beamSchema, TableSchema clickHouseSchema) {
    this.beamSchema = beamSchema;
    this.clickHouseSchema = clickHouseSchema;
  }

  @Setup
  public void setup() {
    columnMap =
        clickHouseSchema.columns().stream()
            .collect(Collectors.toMap(TableSchema.Column::name, column -> column));
  }

  @ProcessElement
  public void processElement(@Element TableRow tableRow, OutputReceiver<Row> out) {
    Row.Builder rowBuilder = Row.withSchema(beamSchema);

    for (Schema.Field field : beamSchema.getFields()) {
      String fieldName = field.getName();
      Object value = tableRow.get(fieldName);
      TableSchema.ColumnType columnType =
          columnMap.get(fieldName) != null ? columnMap.get(fieldName).columnType() : null;

      if (columnType == null) {
        throw new IllegalArgumentException("Couldn't infer type for field: " + fieldName);
      }

      if (value != null) {
        if (columnType.typeName() == TableSchema.ColumnType.FLOAT32.typeName()) {
          rowBuilder.addValue(Float.valueOf(value.toString()));
        } else if (columnType.typeName() == TableSchema.ColumnType.FLOAT64.typeName()) {
          rowBuilder.addValue(Double.valueOf(value.toString()));
        } else if (columnType.typeName() == TableSchema.ColumnType.DATETIME.typeName()
            || columnType.typeName() == TableSchema.ColumnType.DATE.typeName()) {
          rowBuilder.addValue(new DateTime(value.toString()));
        } else if (Objects.equals(columnType.typeName().toString(), "ARRAY")) {
          if (((ArrayList<?>) value).isEmpty()) {
            rowBuilder.addValue(value);
          } else {
            TableSchema.ColumnType finalColumnType = columnType;
            rowBuilder.addValue(
                ((ArrayList<?>) value)
                    .stream()
                        .map(
                            v ->
                                TableSchema.ColumnType.parseDefaultExpression(
                                    finalColumnType.arrayElementType(), v.toString()))
                        .collect(Collectors.toList()));
          }
        } else {
          rowBuilder.addValue(
              TableSchema.ColumnType.parseDefaultExpression(columnType, value.toString()));
        }
      } else {
        rowBuilder.addValue(null);
      }
    }

    Row row = rowBuilder.build();
    out.output(row);
  }
}
