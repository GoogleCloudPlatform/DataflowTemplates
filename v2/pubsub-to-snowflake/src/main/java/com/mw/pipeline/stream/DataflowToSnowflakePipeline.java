package com.mw.pipeline.stream;

import com.mw.pipeline.options.StreamOptions;
import com.mw.pipeline.transforms.StringTransform;
import com.mw.pipeline.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import snowflake.mappers.SimpleMapper;

public class DataflowToSnowflakePipeline {

    public static PipelineResult run(String... args) {


        StreamOptions options = PipelineOptionsFactory.fromArgs().withoutStrictParsing()
                .fromArgs(args)
                .as(StreamOptions.class);
        options.setStreaming(true);

        final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = SnowflakeIO.DataSourceConfiguration
                .create()
                .withKeyPairRawAuth(KMSDecrypt(options.getUsername(), options.getKMSEncryptionKey()), KMSDecrypt(options.getRawPrivateKey(), options.getKMSEncryptionKey()), KMSDecrypt(options.getPrivateKeyPassphrase(), options.getKMSEncryptionKey()))
                .withRole(options.getRole())
                .withServerName(options.getServerName())
                .withDatabase(options.getDatabase())
                .withWarehouse(options.getWarehouse())
                .withSchema(options.getSchema());
        options.getInputSubscription();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read PubSub Events",
                        PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))

                .apply("Convert To Strings",
                        ParDo.of(new StringTransform()))
                .apply("Stream To Snowflake",
                        SnowflakeIO.<String>write()
                                .to(options.getTable())
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withStorageIntegrationName(options.getStorageIntegrationName())
                                .withStagingBucketName(options.getStagingBucketName())
                                .withUserDataMapper(new SimpleMapper().mapper())
                                .withSnowPipe(options.getSnowPipe()));

        return pipeline.run();
    }


    public static void main(String[] args) {
        run(args);
    }

    private static ValueProvider<String> KMSDecrypt(
            ValueProvider<String> plaintextValue, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(plaintextValue, kmsKey);
    }

}



        
