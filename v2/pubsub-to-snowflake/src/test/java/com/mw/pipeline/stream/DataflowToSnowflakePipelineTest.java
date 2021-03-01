package com.mw.pipeline.stream;

import com.google.common.collect.ImmutableList;
import com.mw.pipeline.helpers.TestUtil;
import com.mw.pipeline.options.StreamOptions;
import com.mw.pipeline.transforms.StringTransform;
import com.mw.pipeline.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import snowflake.mappers.SimpleMapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class DataflowToSnowflakePipelineTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    private static transient List<PubsubMessage> testMessages;

    private final StreamOptions options;

    public DataflowToSnowflakePipelineTest(){
        this.options = TestUtil.getMockStreamOptions();
    }

    @Test
    public void test_that_no_data_is_lost_in_transform(){
        PipelineOptionsFactory.register(StreamOptions.class);
        StreamOptions options = this.options;
        PCollection<String> pCollection = Pipeline.create(options)
                .apply("Read PubSub Events",
                        Create.of(testMessages))
                .apply("Convert To Strings",
                        ParDo.of(new StringTransform()));

        PAssert.that(pCollection).containsInAnyOrder(Arrays.asList(messages));
    }

    @Test
    public void initTest() {

         PipelineOptionsFactory.register(StreamOptions.class);
         StreamOptions options = this.options;
         final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = getMockSnowflakeConfig(options);
         //pipeline.apply(Create.of(testMessages));
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Read PubSub Events",
                        Create.of(testMessages))

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

        PipelineResult state = pipeline.create(options).run();
        Assert.assertTrue(state.getState().equals(PipelineResult.State.DONE));

    }

    public SnowflakeIO.DataSourceConfiguration getMockSnowflakeConfig(StreamOptions options) {
        return SnowflakeIO.DataSourceConfiguration
                .create()
                .withKeyPairRawAuth(KMSDecrypt(options.getUsername(), options.getKMSEncryptionKey()), KMSDecrypt(options.getRawPrivateKey(), options.getKMSEncryptionKey()), KMSDecrypt(options.getPrivateKeyPassphrase(), options.getKMSEncryptionKey()))
                .withRole(options.getRole())
                .withServerName(options.getServerName())
                .withDatabase(options.getDatabase())
                .withWarehouse(options.getWarehouse())
                .withSchema(options.getSchema());
    }

    private static ValueProvider<String> KMSDecrypt(
            ValueProvider<String> plaintextValue, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(plaintextValue, kmsKey);
    }

    @Before
    public void setUp() {
        testMessages = ImmutableList.copyOf(Arrays.asList(getPubsubMessage(messages[0]), getPubsubMessage(messages[1]), getPubsubMessage(messages[2])));
    }

    String messages[] = {
            "55dbd56ee03749f8920b7b330ae2e6e41d3f414a,b753b6afd94c77370e97976c023d2729fa586998733fb91e7f28cc4e1c61df444c2640f6ad6369935a800c70372f1b986b525261d0db025290ee03fbf4474050,2013-11-07 18:00:00 UTC",
            "462461d7af9a32000feae87ce851bac230b2f134,79196d52c7dadbc76af237acc709a75cf939ec5097d9ec3f649ec13d1f6a7695efe4f316b705ee09a43c7f45db83c5cc3e05e08bea421b9beb81f7131c988418,2013-11-25 12:45:00 UTC",
            "24f183cf1ed1c21486aa951f1036bfaa46dd1d9c,f26ae0554d3695acf0dea96c2b54df57af02bcb1e11fc4d4c873b828c3f35d14e29af04ac7cd459f369436f452e7730e5a033c901a6a9c5817c1fd892ba3f743,2013-11-18 02:15:00 UTC"
    };

    private PubsubMessage getPubsubMessage(String payload) {

        return new PubsubMessage(payload.getBytes(), null);
    }

}
