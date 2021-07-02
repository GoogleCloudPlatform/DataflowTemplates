package com.mw.pipeline.helpers;

import com.mw.pipeline.options.StreamOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

public class TestUtil {
    public static StreamOptions getMockStreamOptions(){
        StreamOptions options = PipelineOptionsFactory.fromArgs().withoutStrictParsing()
                .as(StreamOptions.class);
        options.setUsername(ValueProvider.StaticValueProvider.of("dummy"));
        options.setRole(ValueProvider.StaticValueProvider.of("dummy"));
        options.setDatabase(ValueProvider.StaticValueProvider.of("dummy"));
        options.setSchema(ValueProvider.StaticValueProvider.of("dummy"));
        options.setWarehouse(ValueProvider.StaticValueProvider.of("dummy"));
        options.setStagingBucketName(ValueProvider.StaticValueProvider.of("dummy"));
        options.setServerName(ValueProvider.StaticValueProvider.of("dummy"));
        options.setInputSubscription(ValueProvider.StaticValueProvider.of("dummy"));
        options.setTable(ValueProvider.StaticValueProvider.of("dummy"));
        options.setSnowPipe(ValueProvider.StaticValueProvider.of("dummy"));
        options.setStorageIntegrationName(ValueProvider.StaticValueProvider.of("dummy"));
        options.setRawPrivateKey(ValueProvider.StaticValueProvider.of("dummy"));
        options.setPrivateKeyPassphrase(ValueProvider.StaticValueProvider.of("dummy"));
        options.setKMSEncryptionKey(ValueProvider.StaticValueProvider.of("Dummy"));
        options.setStreaming(true);
        return options;
    }
}
