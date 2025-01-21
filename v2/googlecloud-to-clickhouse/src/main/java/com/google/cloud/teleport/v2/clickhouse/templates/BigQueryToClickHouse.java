package com.google.cloud.teleport.v2.clickhouse.templates;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.clickhouse.options.BigQueryToClickHouseOptions;
import com.google.cloud.teleport.v2.clickhouse.utils.ClickHouseConverts;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import java.util.ArrayList;
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

            String clickHouseJDBCURL = ClickHouseConverts.setJDBCCredentials(options.getJdbcUrl(), options.getClickHouseUsername(), options.getClickHousePassword());
            TableSchema clickHouseSchema = ClickHouseIO.getTableSchema(clickHouseJDBCURL, options.getClickHouseTable());
            Schema beamSchema = TableSchema.getEquivalentSchema(clickHouseSchema);


            // Create the pipeline.
            Pipeline pipeline = Pipeline.create(options);

            /*
             * Step #1: Read from BigQuery. If a query is provided then it is used to get the TableRows.
             */
            PCollection<TableRow> tableRows =
                    pipeline
                            .apply(
                                    "Read From Big Query",
                                    BigQueryConverters.ReadBigQueryTableRows.newBuilder()
                                            .setOptions(options.as(BigQueryToClickHouseOptions.class))
                                            .build());


            // Step 2: Transform TableRow to Row
            PCollection<Row> rows = tableRows.apply("Convert to Beam Row", ParDo.of(new DoFn<TableRow, Row>() {
                @ProcessElement
                public void processElement(@Element TableRow tableRow, OutputReceiver<Row> out) {

                    Row.Builder rowBuilder = Row.withSchema(beamSchema);


                    for (Schema.Field field : beamSchema.getFields()) {
                        String fieldName = field.getName();
                        Object value = tableRow.get(fieldName);
                        TableSchema.ColumnType columnType = null;
                        for (TableSchema.Column column : clickHouseSchema.columns()) {
                            if (column.name().equals(fieldName)) {
                                columnType = column.columnType();
                                break;
                            }
                        }

                        if (columnType == null) {
                            throw new IllegalArgumentException("Couldn't infer type for field: " + fieldName);
                        }
                        if (value != null) {
                            // we handle the conversion manually for unimplemented types until this issue would be solved:
                            // https://github.com/apache/beam/issues/33692
                            if (columnType.typeName() == TableSchema.ColumnType.FLOAT32.typeName()) {
                                rowBuilder.addValue(Float.valueOf(value.toString()));
                            } else if (columnType.typeName() == TableSchema.ColumnType.FLOAT64.typeName()) {
                                rowBuilder.addValue(Double.valueOf(value.toString()));
                            } else if (columnType.typeName() == TableSchema.ColumnType.DATETIME.typeName()) {
                                rowBuilder.addValue(new DateTime(value.toString()));
                            } else if (Objects.equals(columnType.typeName().toString(), "ARRAY")) {
                                if (((ArrayList<?>) value).isEmpty()) {
                                    rowBuilder.addValue(value);
                                } else {
                                    TableSchema.ColumnType finalColumnType = columnType;
                                    rowBuilder.addValue(
                                            ((ArrayList<?>) value)
                                                    .stream()
                                                    .map(v -> TableSchema.ColumnType.parseDefaultExpression(finalColumnType.arrayElementType(), v.toString()))
                                                    .collect(Collectors.toList())
                                    );
                                }

                            } else {
                                rowBuilder.addValue(TableSchema.ColumnType.parseDefaultExpression(columnType, value.toString()));
                            }

                        } else {
                            rowBuilder.addValue(null); // Handle nulls gracefully
                        }
                    }
                    Row row = rowBuilder.build();
                    out.output(row);
                }
            })).setRowSchema(beamSchema);

            ClickHouseIO.Write clickHouseWriter = ClickHouseIO.write(clickHouseJDBCURL, options.getClickHouseTable());

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
