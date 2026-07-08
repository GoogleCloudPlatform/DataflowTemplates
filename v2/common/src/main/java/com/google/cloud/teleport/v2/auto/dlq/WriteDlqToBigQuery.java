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
package com.google.cloud.teleport.v2.auto.dlq;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.BlockConstants;
import com.google.cloud.teleport.v2.auto.dlq.WriteDlqToBigQuery.BigQueryDlqOptions;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.ResourceUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollectionTuple;

public class WriteDlqToBigQuery implements TemplateTransform<BigQueryDlqOptions> {

  public interface BigQueryDlqOptions extends PipelineOptions {

    @TemplateParameter.BigQueryTable(
        order = 4,
        optional = true,
        groupName = "Target",
        description =
            "Table for messages failed to reach the output table (i.e., Deadletter table)",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched"
                + " schema, malformed json) are written to this table. It should be in the format"
                + " of \"your-project-id:your-dataset.your-table-name\". If it doesn't exist, it"
                + " will be created during pipeline execution. If not specified,"
                + " \"{outputTableSpec}_error_records\" is used instead.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String value);
  }

  @Consumes(
      value = FailsafeElement.class,
      types = {PubsubMessage.class, String.class})
  public void writeDLQToBigQueryForPubsubMessage(
      PCollectionTuple input, BigQueryDlqOptions options) {
    input
        .get(BlockConstants.ERROR_TAG_PS)
        .apply(
            ErrorConverters.WritePubsubMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getOutputDeadletterTable())
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());
  }

  @Consumes(
      value = FailsafeElement.class,
      types = {String.class, String.class})
  public void writeDLQToBigQueryForString(PCollectionTuple input, BigQueryDlqOptions options) {
    input
        .get(BlockConstants.ERROR_TAG_STR)
        .apply(
            ErrorConverters.WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getOutputDeadletterTable())
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());
  }

  @Override
  public Class<BigQueryDlqOptions> getOptionsClass() {
    return BigQueryDlqOptions.class;
  }
}
