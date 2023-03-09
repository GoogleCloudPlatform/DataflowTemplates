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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KinesisToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KinesisToPubsubOptions
    extends PipelineOptions, DataflowPipelineOptions, AwsOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      description = "First Secret ID",
      helpText = "First Secret ID containing aws key id")
  @Validation.Required
  String getSecretId1();

  void setSecretId1(String secretId1);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      description = "Second Secret ID",
      helpText = "Second Secret ID containing aws key id")
  @Validation.Required
  String getSecretId2();

  void setSecretId2(String secretId2);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      description = "AWS Region",
      helpText = "AWS Region")
  @Validation.Required
  @Default.InstanceFactory(AwsRegionFactory.class)
  String getAwsRegion();

  void setAwsRegion(String awsRegion);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description = "Data format of input",
      helpText = "Data format of input")
  @Validation.Required
  String getAwsDataFormat();

  void setAwsDataFormat(String awsDataFormat);

  @TemplateParameter.Text(
      order = 5,
      optional = false,
      description = "Name of the Kinesis Data stream to read from",
      helpText =
          "Name of the Kinesis Data stream to read from."
              + " Enter the full name of the Kinesis Data stream")
  @Validation.Required
  String getKinesisDataStream();

  void setKinesisDataStream(String kinesisDataStream);

  @TemplateParameter.PubsubTopic(
      order = 6,
      description = "Output Pub/Sub topic",
      helpText =
          "The name of the topic to which data should published, "
              + "in the format of 'projects/your-project-id/topics/your-topic-name'",
      example = "projects/your-project-id/topics/your-topic-name")
  @Validation.Required
  String getOutputPubsubTopic();

  void setOutputPubsubTopic(String outputPubsubTopic);
}
