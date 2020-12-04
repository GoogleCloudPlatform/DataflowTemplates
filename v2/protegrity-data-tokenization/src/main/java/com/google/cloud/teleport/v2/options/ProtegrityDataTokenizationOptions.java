/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link ProtegrityDataTokenizationOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface ProtegrityDataTokenizationOptions extends PipelineOptions {
    // Group 1 - Input source
    @Description("Path to data schema (JSON format) in GCS compatible with BigQuery.")
    String getDataSchemaGcsPath();

    void setDataSchemaGcsPath(String dataSchemaGcsPath);

    // Group 1.1 - GCS
    @Description("GCS filepattern for files in bucket to read data from")
    String getInputGcsFilePattern();

    void setInputGcsFilePattern(String inputGcsFilePattern);

    // Group 1.2 - Pub/Sub
    @Description(
            "The Cloud Pub/Sub topic to read from."
                    + "The name should be in the format of "
                    + "projects/<project-id>/topics/<topic-name>.")
    String getPubsubTopic();

    void setPubsubTopic(String pubsubTopic);

    // Group 2 - Output sink
    // Group 2.1 - BigTable
    @Description("Id of the project where the Cloud BigTable instance to write into is located.")
    String getBigTableProjectId();

    void setBigTableProjectId(String bigTableProjectId);

    @Description("Id of the Cloud BigTable instance to write into.")
    String getBigTableInstanceId();

    void setBigTableInstanceId(String bigTableInstanceId);

    @Description("Id of the Cloud BigTable table to write into.")
    String getBigTableTableId();

    void setBigTableTableId(String bigTableTableId);

    @Description("Column name to use as a key in Cloud BigTable.")
    String getBigTableKeyColumnName();

    void setBigTableKeyColumnName(String bigTableKeyColumnName);

    //Group 2.2 - BigQuery
    @Description("Cloud BigQuery table name to write into.")
    String getBigQueryTableName();

    void setBigQueryTableName(String bigQueryTableName);

    //Group 3 - Protegrity specific parameters
    @Description("URI for the API calls to DSG.")
    String getDsgUri();

    void setDsgUri(String dsgUri);

    @Description("Size of the batch to send to DSG per request.")
    Integer getBatchSize();

    void setBatchSize(Integer batchSize);

    @Description("GCS path to the payload configuration file.")
    String getPayloadConfigGcsPath();

    void setPayloadConfigGcsPath(String payloadConfigGcsPath);
}
