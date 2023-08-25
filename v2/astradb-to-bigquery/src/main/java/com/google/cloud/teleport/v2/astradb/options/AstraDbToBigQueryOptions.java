/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.astradb.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link AstraDbToBigQueryOptions} class provides the custom execution options passed by
 * executor at the command-line.
 */
public interface AstraDbToBigQueryOptions {

  /** Options for Reading BigQuery Rows. */
  interface BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {
    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        optional = true,
        helpText =
            "The BigQuery table location to write the output to. "
                + "The table should be in the format `<project>:<dataset>.<table_name>`. "
                + "The table's schema must match the input objects.")
    String getOutputTableSpec();

    @SuppressWarnings("unused")
    void setOutputTableSpec(String outputTableSpec);
  }

  /** Options to Select Records from Cassandra. */
  interface AstraDbSourceOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        description = "Astra token",
        helpText = "Token value or secret resource ID",
        example = "AstraCS:abcdefghij")
    @Validation.Required
    @SuppressWarnings("unused")
    String getAstraToken();

    @SuppressWarnings("unused")
    void setAstraToken(String astraToken);

    @TemplateParameter.Text(
        order = 2,
        description = "Database identifier",
        helpText = "Database unique identifier (uuid)",
        example = "cf7af129-d33a-498f-ad06-d97a6ee6eb7")
    @Validation.Required
    @SuppressWarnings("unused")
    String getAstraDatabaseId();

    @SuppressWarnings("unused")
    void setAstraDatabaseId(String astraDatabaseId);

    @TemplateParameter.Text(
        order = 3,
        description = "Cassandra keyspace",
        regexes = {"^[a-zA-Z0-9][a-zA-Z0-9_]{0,47}$"},
        helpText = "Name of the Cassandra keyspace inside Astra database")
    String getAstraKeyspace();

    @SuppressWarnings("unused")
    void setAstraKeyspace(String astraKeyspace);

    @TemplateParameter.Text(
        order = 4,
        description = "Cassandra table",
        regexes = {"^[a-zA-Z][a-zA-Z0-9_]*$"},
        helpText = "Name of the table inside the Cassandra database",
        example = "my_table")
    @SuppressWarnings("unused")
    String getAstraTable();

    @SuppressWarnings("unused")
    void setAstraTable(String astraTable);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Cassandra CQL Query",
        helpText = "Query to filter rows instead of reading the whole table")
    @SuppressWarnings("unused")
    String getAstraQuery();

    @SuppressWarnings("unused")
    void setAstraQuery(String astraQuery);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Astra Database Region",
        helpText =
            "If not provided, a default is chosen, which is useful with multi-region databases")
    @SuppressWarnings("unused")
    String getAstraDatabaseRegion();

    @SuppressWarnings("unused")
    void setAstraDatabaseRegion(String astraDatabaseRegion);

    @TemplateParameter.Integer(
        order = 7,
        optional = true,
        description = "Token range count",
        helpText = "The minimal number of splits to distribute the query")
    Integer getMinTokenRangesCount();

    @SuppressWarnings("unused")
    void setMinTokenRangesCount(Integer count);
  }
}
