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
