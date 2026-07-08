/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.plugin.sample;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.plugin.sample.AtoBOk.AtoBOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/** Sample template used for testing. */
@Template(
    name = "AtoB",
    displayName = "A to B",
    description = {"Streaming Template that sends A to B.", "But it can also send B to C."},
    category = TemplateCategory.STREAMING,
    optionsClass = AtoBOptions.class,
    preview = true,
    requirements = "Requires the customer to use Dataflow")
public class AtoBOk {

  public interface AtoBOptions {
    @TemplateParameter.BigQueryTable(
        order = 2,
        optional = true,
        description = "Source table",
        helpText = "Table to send data to",
        example = "b")
    String to();

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Target table",
        helpText = "Define where to get data from",
        example = "a")
    String from();

    @TemplateParameter.Boolean(
        order = 3,
        description = "Check if data should be converted.",
        helpText = "Define if A goes to B")
    @Default.Boolean(true)
    Boolean logical();

    @TemplateParameter.Boolean(
        order = 4,
        description = "JSON all caps",
        helpText = "Some JSON property.")
    @Default.Boolean(true)
    String getJSON();

    @TemplateParameter.PubsubSubscription(
        order = 5,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    @Validation.Required
    String getInputSubscription();

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "String default empty",
        helpText = "String that defaults to empty",
        example = "whatever")
    @Default.String("")
    String getEmpty();

    @TemplateParameter.Text(
        order = 7,
        hiddenUi = true,
        description = "N/A",
        helpText = "String that is hidden in the UI")
    @Default.String("")
    String getHiddenParam();

    @TemplateParameter.Boolean(
        order = 8,
        groupName = "Source",
        description = "N/A",
        helpText = "Boolean that has group name")
    @Default.Boolean(false)
    Boolean getParamWithGroupName();

    @TemplateParameter.Boolean(
        order = 9,
        parentName = "paramWithGroupName",
        parentTriggerValues = {"true"},
        description = "N/A",
        helpText = "Boolean that has parent name and parent trigger value")
    @Default.Boolean(false)
    Boolean getParamWithParentName();

    @TemplateParameter.KafkaReadTopic(
        order = 10,
        description = "Kafka input topic",
        helpText = "Kafka topic to read from",
        example =
            "projects/project-foo/locations/us-central1/clusters/cluster-bar/topics/topic-baz")
    String getInputKafkaReadTopic();

    @TemplateParameter.KafkaWriteTopic(
        order = 10,
        description = "Kafka input topic",
        helpText = "Kafka topic to read from",
        example =
            "projects/project-foo/locations/us-central1/clusters/cluster-bar/topics/topic-baz")
    String getInputKafkaWriteTopic();

    @TemplateParameter.GcsReadBucket(
        order = 11,
        description = "Cloud Storage Bucket to read from",
        helpText = "Cloud Storage Bucket to read from")
    @Default.String("")
    String getGcsReadBucket();

    @TemplateParameter.GcsWriteBucket(
        order = 12,
        description = "Cloud Storage Bucket to write",
        helpText = "Cloud Storage Bucket to write")
    @Default.String("")
    String getGcsWriteBucket();

    @TemplateParameter.JavascriptUdfFile(
        order = 13,
        description = "JavaScript UDF in Cloud Storage",
        helpText = "JavaScript UDF in Cloud Storage")
    @Default.String("")
    String javascriptUdfFile();

    @TemplateParameter.MachineType(
        order = 14,
        description = "N/A",
        helpText = "String that is machine type")
    @Default.String("")
    String getMachineType();

    @TemplateParameter.ServiceAccount(
        order = 15,
        description = "Google Cloud service account email",
        helpText = "Google Cloud service account email",
        example = "some@name.iam.gserviceaccount.com")
    @Default.String("")
    String getServiceAccount();

    @TemplateParameter.WorkerRegion(
        order = 16,
        description = "Worker region",
        helpText = "Worker region",
        example = "us-west1")
    @Default.String("")
    String getWorkerRegion();

    @TemplateParameter.WorkerZone(
        order = 17,
        description = "Worker zone",
        helpText = "Worker zone",
        example = "us-west1-a")
    @Default.String("")
    String getWorkerZone();
  }
}
