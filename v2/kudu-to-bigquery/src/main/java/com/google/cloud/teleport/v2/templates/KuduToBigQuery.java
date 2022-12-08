/*
 * Copyright (C) 2020 Google LLC
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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.common.base.Splitter;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.kudu.KuduIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

/** Templated pipeline to read data from Kudu, and write it to BigQuery. */
public class KuduToBigQuery {

  /** Options supported by {@link com.google.cloud.teleport.v2.templates.KuduToBigQuery}. */
  public interface KuduToBigQueryOptions extends PipelineOptions, BigQueryOptions {

    @Description("Master addresses")
    @Validation.Required
    String getKuduMasterAddresses();

    void setKuduMasterAddresses(String value);

    @Description("Kudu Tables")
    @Validation.Required
    String getkuduTableSource();

    void setkuduTableSource(String value);

    @Description("Kudu Columns Included")
    @Default.String("")
    String getIncludeColumns();

    void setIncludeColumns(String value);

    @Description("BigQuery destination Table")
    @Validation.Required
    String getBigQueryTableSpec();

    void setBigQueryTableSpec(String value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KuduToBigQuery#run(KuduToBigQueryOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    KuduToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KuduToBigQueryOptions.class);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(KuduToBigQueryOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Read RowRecords from Kudu
     *      - Load all columns if not defined in includedColumn parameter
     *      - Load only included Columns if defined in includedColumns parameter
     *  2) Transform the RowRecords into TableRows using fnKuduConverter
     *  3) Write records out to BigQuery
     */

    // Step 1. Read Kudu RowRecords
    KuduIO.Read<TableRow> readKuduRecords =
        KuduIO.<TableRow>read()
            .withMasterAddresses(options.getKuduMasterAddresses())
            .withTable(options.getkuduTableSource());

    /*
     * Read only selected columns if defined in includedColumns parameter.
     * Input must be in comma separated values in string "col_a,col_b,col_c"
     */
    if (!options.getIncludeColumns().equals("")) {
      String includedColumns = options.getIncludeColumns().replaceAll("\\s+", "");
      List<String> includedColumnsList = Splitter.on(',').splitToList(includedColumns);

      readKuduRecords.withProjectedColumns(includedColumnsList);
    }

    // Step 2. A Serializable Function to Transform the RowRecords into TableRows
    // used by KuduIO.read withParseFn()
    PCollection<TableRow> tableRows =
        pipeline.apply(
            readKuduRecords.withParseFn(KuduConverters.of()).withCoder(TableRowJsonCoder.of()));

    // Step 3. Write records out to BigQuery
    tableRows.apply(
        "Write to BigQuery",
        BigQueryIO.writeTableRows()
            .withoutValidation()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .to(options.getBigQueryTableSpec()));

    return pipeline.run();
  }

  /*
   * Convert Kudu RowResult to BigQuery TableRow.
   * Each columns in Kudu row will be converted into BigQuery columns.
   * If the column's datatype is UNIXTIME_MICROS(timestamp in kudu) convert to nanoseconds.
   */
  static class KuduConverters implements SerializableFunction<RowResult, TableRow> {

    public static KuduConverters of() {
      return INSTANCE;
    }

    private static final KuduConverters INSTANCE = new KuduConverters();

    @Override
    public TableRow apply(RowResult input) {
      TableRow outputTableRow = new TableRow();
      Schema kuduSchema = input.getSchema();

      for (int i = 0; i < kuduSchema.getColumnCount(); i++) {
        String columnName = kuduSchema.getColumnByIndex(i).getName();

        if (kuduSchema.getColumnByIndex(i).getType() == Type.UNIXTIME_MICROS) {
          outputTableRow.set(columnName, convertNanoSecondsToMilliSeconds(input, i));
        } else {
          // For other datatypes return the object value
          outputTableRow.set(columnName, input.getObject(i));
        }
      }
      return outputTableRow;
    }
  }

  /*
   * Handle Timestamp nanoseconds to milliseconds.
   * Will return null if the column value is null.
   *
   * @param input the kudu row that will be converted.
   * @param colIndex the column index location from the kudu row.
   *
   * @return Long The nano seconds value.
   */
  private static Long convertNanoSecondsToMilliSeconds(RowResult input, int colIndex) {
    if (!input.isNull(colIndex)) {
      Long timestamp = input.getLong(colIndex) / 1000000;
      return timestamp;
    } else {
      return null;
    }
  }
}
