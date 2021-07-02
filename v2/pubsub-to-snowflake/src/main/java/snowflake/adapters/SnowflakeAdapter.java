package snowflake.adapters;

import com.mw.pipeline.options.StreamOptions;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SnowflakeAdapter {
    public static SnowflakeIO.DataSourceConfiguration getConfig(String... args) {
        SnowflakePipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StreamOptions.class);
        options.setStreaming(true);
        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = buildConfig(options);
        return dataSourceConfiguration;
    }

    private static SnowflakeIO.DataSourceConfiguration buildConfig(SnowflakePipelineOptions options) {

        final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = SnowflakeIO.DataSourceConfiguration
                .create()
                .withKeyPairRawAuth(options.getUsername(), options.getRawPrivateKey(), options.getPrivateKeyPassphrase())
                //.withUrl(options.getUrl())
                .withRole(options.getRole())
                .withServerName(options.getServerName())
                .withDatabase(options.getDatabase())
                .withWarehouse(options.getWarehouse())
                .withSchema(options.getSchema());
        return dataSourceConfiguration;
    }


}
