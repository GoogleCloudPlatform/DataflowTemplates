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
package com.google.cloud.teleport.v2.bigtable.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.bigtable.BigtableIO}.
 */
public interface BigtableCommonOptions extends GcpOptions {

  @TemplateParameter.Integer(
      order = 1,
      optional = true,
      description = "The timeout for an RPC attempt in milliseconds",
      helpText = "This sets the timeout for an RPC attempt in milliseconds")
  Integer getBigtableRpcAttemptTimeoutMs();

  void setBigtableRpcAttemptTimeoutMs(Integer value);

  @TemplateParameter.Integer(
      order = 2,
      optional = true,
      description = "The total timeout for an RPC operation in milliseconds",
      helpText = "This sets the total timeout for an RPC operation in milliseconds")
  Integer getBigtableRpcTimeoutMs();

  void setBigtableRpcTimeoutMs(Integer value);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "The additional retry codes",
      helpText = "This sets the additional retry codes, separated by ','",
      example = "RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED")
  String getBigtableAdditionalRetryCodes();

  void setBigtableAdditionalRetryCodes(String value);

  /** Provides {@link PipelineOptions} to write records to a Bigtable table. */
  interface WriteOptions extends BigtableCommonOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Bigtable Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    @Validation.Required
    String getBigtableWriteInstanceId();

    void setBigtableWriteInstanceId(String value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Bigtable Table ID",
        helpText = "The ID of the Cloud Bigtable table to write")
    @Validation.Required
    String getBigtableWriteTableId();

    void setBigtableWriteTableId(String value);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[-_.a-zA-Z0-9]+"},
        description = "The Bigtable Column Family",
        helpText = "This specifies the column family to write data into")
    @Validation.Required
    String getBigtableWriteColumnFamily();

    void setBigtableWriteColumnFamily(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Bigtable App Profile",
        helpText =
            "Bigtable App Profile to use for the export. The default for this parameter "
                + "is the Bigtable instance's default app profile")
    @Default.String("default")
    String getBigtableWriteAppProfile();

    void setBigtableWriteAppProfile(String value);

    @TemplateParameter.ProjectId(
        order = 5,
        optional = true,
        description = "Bigtable Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want "
                + "to write data to.")
    String getBigtableWriteProjectId();

    void setBigtableWriteProjectId(String value);

    @TemplateParameter.Integer(
        order = 6,
        optional = true,
        description = "Bigtable's latency target in milliseconds for latency-based throttling",
        helpText = "This enables latency-based throttling and specifies the target latency")
    Integer getBigtableBulkWriteLatencyTargetMs();

    void setBigtableBulkWriteLatencyTargetMs(Integer value);

    @TemplateParameter.Integer(
        order = 7,
        optional = true,
        description = "The max number of row keys in a Bigtable batch write operation",
        helpText = "This sets the max number of row keys in a Bigtable batch write operation")
    Integer getBigtableBulkWriteMaxRowKeyCount();

    void setBigtableBulkWriteMaxRowKeyCount(Integer value);

    @TemplateParameter.Integer(
        order = 8,
        optional = true,
        description = "The max amount of bytes in a Bigtable batch write operation",
        helpText = "This sets the max amount of bytes in a Bigtable batch write operation")
    Integer getBigtableBulkWriteMaxRequestSizeBytes();

    void setBigtableBulkWriteMaxRequestSizeBytes(Integer value);
  }

  interface ReadOptions extends BigtableCommonOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Source Bigtable Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    @Validation.Required
    String getBigtableReadInstanceId();

    void setBigtableReadInstanceId(String value);

    @TemplateParameter.Text(
        order = 2,
        description = "Source Cloud Bigtable table ID",
        helpText = "The Cloud Bigtable table to read from.")
    @Validation.Required
    String getBigtableReadTableId();

    void setBigtableReadTableId(String value);

    @TemplateParameter.ProjectId(
        order = 3,
        optional = true,
        description = "Source Cloud Bigtable Project ID",
        helpText =
            "Project to read Cloud Bigtable data from. The default for this parameter is the "
                + "project where the Dataflow pipeline is running.")
    @Default.String("")
    String getBigtableReadProjectId();

    void setBigtableReadProjectId(String projectId);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"[a-z][a-z0-9\\-_]+[a-z0-9]"},
        description = "Bigtable App Profile",
        helpText =
            "Bigtable App Profile to use for reads. The default for this parameter "
                + "is the Bigtable instance's default app profile")
    @Default.String("default")
    String getBigtableReadAppProfile();

    void setBigtableReadAppProfile(String value);
  }

  interface ReadChangeStreamOptions extends BigtableCommonOptions.ReadOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = true,
        description = "Cloud Bigtable change streams metadata instance ID",
        helpText =
            "The Cloud Bigtable instance to use for the change streams connector metadata table.")
    @Default.String("")
    String getBigtableChangeStreamMetadataInstanceId();

    void setBigtableChangeStreamMetadataInstanceId(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "Cloud Bigtable change streams metadata table ID",
        helpText =
            "The Cloud Bigtable change streams connector metadata table ID to use. If not "
                + "provided, a Cloud Bigtable change streams connector metadata table will automatically be "
                + "created during the pipeline flow.")
    @Default.String("")
    String getBigtableChangeStreamMetadataTableTableId();

    void setBigtableChangeStreamMetadataTableTableId(String value);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[a-z][a-z0-9\\-_]+[a-z0-9]"},
        description = "Cloud Bigtable application profile ID",
        helpText = "The application profile is used to distinguish workload in Cloud Bigtable")
    @Validation.Required
    String getBigtableChangeStreamAppProfile();

    void setBigtableChangeStreamAppProfile(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description =
            "Bigtable change streams charset name when reading values and column qualifiers",
        helpText =
            "Bigtable change streams charset name when reading values and column qualifiers. "
                + "Default is UTF-8")
    @Default.String("UTF-8")
    String getBigtableChangeStreamCharset();

    void setBigtableChangeStreamCharset(String value);

    @TemplateParameter.DateTime(
        order = 5,
        optional = true,
        description = "The timestamp to read change streams from",
        helpText =
            "The starting DateTime, inclusive, to use for reading change streams "
                + "(https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the "
                + "timestamp when the pipeline starts.")
    @Default.String("")
    String getBigtableChangeStreamStartTimestamp();

    void setBigtableChangeStreamStartTimestamp(String startTimestamp);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Cloud Bigtable change streams column families to ignore",
        helpText =
            "A comma-separated list of column family names changes to which won't be captured")
    @Default.String("")
    String getBigtableChangeStreamIgnoreColumnFamilies();

    void setBigtableChangeStreamIgnoreColumnFamilies(String value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Cloud Bigtable change streams columns to ignore",
        helpText = "A comma-separated list of column names changes to which won't be captured")
    @Default.String("")
    String getBigtableChangeStreamIgnoreColumns();

    void setBigtableChangeStreamIgnoreColumns(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "A unique name of the client pipeline",
        helpText =
            "Allows to resume processing from the point where a "
                + "previously running pipeline stopped")
    String getBigtableChangeStreamName();

    void setBigtableChangeStreamName(String value);

    @TemplateParameter.Boolean(
        order = 9,
        optional = true,
        description = "Resume streaming with the same change stream name",
        helpText =
            "When set to true< a new pipeline will resume processing from the point at which "
                + "a previously running pipeline with the same bigtableChangeStreamName stopped. "
                + "If pipeline with the given bigtableChangeStreamName never ran in the past, a "
                + "new pipeline will fail to start. When set to false a new pipeline will be "
                + "started. If pipeline with the same bigtableChangeStreamName already ran in "
                + "the past for the given source, a new pipeline will fail to start. "
                + "Defaults to false")
    @Default.Boolean(false)
    Boolean getBigtableChangeStreamResume();

    void setBigtableChangeStreamResume(Boolean useBase64Value);
  }
}
