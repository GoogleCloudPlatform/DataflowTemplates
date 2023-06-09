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

import com.google.cloud.teleport.metadata.TemplateParameter;
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

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format "
                + "<project>:<dataset>.<table_name>. The table's schema must match input objects.")
    @Required
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {"WRITE_APPEND", "WRITE_EMPTY", "WRITE_TRUNCATE"},
        optional = true,
        description = "Write Disposition to use for BigQuery",
        helpText =
            "BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @TemplateParameter.Enum(
        order = 3,
        enumOptions = {"CREATE_IF_NEEDED", "CREATE_NEVER"},
        optional = true,
        description = "Create Disposition to use for BigQuery",
        helpText = "BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER.")
    @Default.String("CREATE_IF_NEEDED")
    String getCreateDisposition();

    void setCreateDisposition(String createDisposition);

    /**
     * @deprecated Not being used.
     */
    @Description(
        "Set partition type to use for BigQuery.  "
            + "Currently 'None' or 'Time' can be available"
            + "Default: None")
    @Default.String("None")
    @Deprecated
    String getPartitionType();

    /**
     * @deprecated Not being used.
     */
    @Deprecated
    void setPartitionType(String partitionType);

    /**
     * @deprecated Not being used.
     */
    @Description("Set partition column name.  " + "Default: _PARTITIONTIME")
    @Default.String("_PARTITIONTIME")
    @Deprecated
    String getPartitionCol();

    /**
     * @deprecated Not being used.
     */
    @Deprecated
    void setPartitionCol(String partitionCol);
  }
}
