/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates.common;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/** Options for Teleport KinesisIO. */
public class KinesisConverters {

  /** Options for Kinesis Stream. */
  public interface KinesisStreamOptions extends PipelineOptions {
    @Description("The AWS Kinesis Stream")
    ValueProvider<String> getAwsKinesisStream();

    void setAwsKinesisStream(ValueProvider<String> awsKinesisStream);
  }

  /** Options for Kinesis PartitionKey. */
  public interface KinesisPartitionKeyOptions extends PipelineOptions {
    @Description("The AWS Kinesis PartitionKey")
    ValueProvider<String> getAwsKinesisPartitionKey();

    void setAwsKinesisPartitionKey(ValueProvider<String> awsKinesisPartitionKey);
  }

  /** Options for Kinesis AccessKey. */
  public interface KinesisAccessKeyOptions extends PipelineOptions {
    @Description(
        "The AWS Kinesis AWS AccessKey")
    ValueProvider<String> getAwsAccessKey();

    void setAwsAccessKey(ValueProvider<String> awsAccessKey);
  }
  /** Options for Kinesis SecretKey. */
  public interface KinesisSecretKeyOptions extends PipelineOptions {
    @Description(
        "The AWS Kinesis AWS SecretKey")
    ValueProvider<String> getAwsSecretKey();

    void setAwsSecretKey(ValueProvider<String> awsSecretKey);
  }
  /** Options for Kinesis Region. */
  public interface KinesisRegionOptions extends PipelineOptions {
    @Description(
        "The AWS Kinesis AWS Region")
    ValueProvider<String> getAwsKinesisRegion();

    void setAwsKinesisRegion(ValueProvider<String> awsKinesisRegion);
  }
}
