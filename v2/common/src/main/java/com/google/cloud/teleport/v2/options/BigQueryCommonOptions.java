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
package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}.
 */
public final class BigQueryCommonOptions {

  private BigQueryCommonOptions() {}

  /** Provides {@link PipelineOptions} to write records to a BigQuery table. */
  public interface WriteOptions extends PipelineOptions {

    @Description(
        "Output BigQuery table spec. "
            + "The name should be in the format: "
            + "<project>:<dataset>.<table_name>.")
    @Required
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @Description("Write disposition to use for BigQuery. " + "Default: WRITE_APPEND")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @Description("Create disposition to use for BigQuery. " + "Default: CREATE_IF_NEEDED")
    @Default.String("CREATE_IF_NEEDED")
    String getCreateDisposition();

    void setCreateDisposition(String createDisposition);

    @Description(
        "Set partition type to use for BigQuery.  "
            + "Currently 'None' or 'Time' can be available"
            + "Default: None")
    @Default.String("None")
    String getPartitionType();

    void setPartitionType(String partitionType);

    @Description("Set partition column name.  " + "Default: _PARTITIONTIME")
    @Default.String("_PARTITIONTIME")
    String getPartitionCol();

    void setPartitionCol(String partitionCol);
  }
}
