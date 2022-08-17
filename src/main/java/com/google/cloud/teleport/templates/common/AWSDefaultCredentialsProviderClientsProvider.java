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
package com.google.cloud.teleport.templates.common;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.apache.beam.sdk.io.kinesis.AWSClientsProvider;

/** Common code for AWS Credentials. */
public class AWSDefaultCredentialsProviderClientsProvider implements AWSClientsProvider {
  private final Regions region;

  public AWSDefaultCredentialsProviderClientsProvider(Regions region) {
    this.region = region;
  }

  @Override
  public AmazonKinesis getKinesisClient() {
    return AmazonKinesisClientBuilder.standard().withRegion(region).build();
  }

  @Override
  public AmazonCloudWatch getCloudWatchClient() {
    return AmazonCloudWatchClient.builder().withRegion(region).build();
  }

  @Override
  public IKinesisProducer createKinesisProducer(KinesisProducerConfiguration config) {
    config.setRegion(region.getName());
    return new KinesisProducer(config);
  }
}
