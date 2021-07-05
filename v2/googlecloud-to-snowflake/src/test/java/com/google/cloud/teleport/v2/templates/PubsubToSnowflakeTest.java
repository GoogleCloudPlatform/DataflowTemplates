/*
 * Copyright (C) 2021 Google Inc.
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.snowflake.util.DecryptUtil.getRawEncrypted;
import static com.google.cloud.teleport.v2.snowflake.util.SimpleMapper.getCsvUserDataMapper;

import com.google.cloud.teleport.v2.helpers.PubsubToSnowflakeUtil;
import com.google.cloud.teleport.v2.snowflake.options.PubSubToSnowflakeOptions;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/** Test cases for the {@link PubsubToSnowflakeTest} class. */
@RunWith(JUnit4.class)
public class PubsubToSnowflakeTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    private static transient List<String> testMessages;

    private final PubSubToSnowflakeOptions options;

    public PubsubToSnowflakeTest(){
        this.options = PubsubToSnowflakeUtil.getMockPubSubToSnowflakeOptions();
    }

    /** Tests the execution of pipeline with specified options. */
    @Test
    public void initTest() {

         PipelineOptionsFactory.register(PubSubToSnowflakeOptions.class);
         PubSubToSnowflakeOptions options = this.options;
         final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = getMockSnowflakeConfig(options);

         String table = getRawEncrypted(options.getTable(),options.getTokenKMSEncryptionKey());
         String storageIntegrationName = getRawEncrypted(options.getStorageIntegrationName(),options.getTokenKMSEncryptionKey());
         String stagingBucketName = getRawEncrypted(options.getStagingBucketName(),options.getTokenKMSEncryptionKey());
         String snowPipe = getRawEncrypted(options.getSnowPipe(),options.getTokenKMSEncryptionKey());
         String sourceFormat = getRawEncrypted(options.getSourceFormat(),options.getTokenKMSEncryptionKey());
         
         Pipeline pipeline = Pipeline.create(options);
         pipeline
                .apply("Read PubSub Events as Strings",
                        Create.of(testMessages))
                .apply("Stream To Snowflake",
                        SnowflakeIO.<String>write()
                                .to(table)
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withStorageIntegrationName(storageIntegrationName)
                                .withStagingBucketName(stagingBucketName)
                                .withUserDataMapper(getCsvUserDataMapper(sourceFormat))
                                .withSnowPipe(snowPipe));

        PipelineResult state = pipeline.create(options).run();
        Assert.assertTrue(state.getState().equals(PipelineResult.State.DONE));

    }

    /** Helper method to get mock Snowflake Datasource configuration. */
    public SnowflakeIO.DataSourceConfiguration getMockSnowflakeConfig(PubSubToSnowflakeOptions options) {
    	
        String userName = getRawEncrypted(options.getUsername(),options.getTokenKMSEncryptionKey());
        String password = getRawEncrypted(options.getPassword(),options.getTokenKMSEncryptionKey());
        String oauthToken = getRawEncrypted(options.getOauthToken(),options.getTokenKMSEncryptionKey());
        String rawPrivateKey = getRawEncrypted(options.getRawPrivateKey(),options.getTokenKMSEncryptionKey());
        String privateKeyPassphrase = getRawEncrypted(options.getPrivateKeyPassphrase(),options.getTokenKMSEncryptionKey());
        String role = getRawEncrypted(options.getRole(),options.getTokenKMSEncryptionKey());
        String serverName = getRawEncrypted(options.getServerName(),options.getTokenKMSEncryptionKey());
        String database = getRawEncrypted(options.getDatabase(),options.getTokenKMSEncryptionKey());
        String warehouse = getRawEncrypted(options.getWarehouse(),options.getTokenKMSEncryptionKey());
        String schema = getRawEncrypted(options.getSchema(),options.getTokenKMSEncryptionKey());
                
    	return SnowflakeIO.DataSourceConfiguration.create()
    			.withKeyPairRawAuth(userName,rawPrivateKey,privateKeyPassphrase)
    			.withUsernamePasswordAuth(userName, password)
    			.withOAuth(oauthToken)
                .withRole(role)
                .withServerName(serverName)
                .withDatabase(database)
                .withWarehouse(warehouse)
                .withSchema(schema);
    }

    @Before
    public void setUp() {
        testMessages = ImmutableList.copyOf(Arrays.asList(messages[0],messages[1],messages[2]));
    }

    String[] messages = {
            "55dbd56ee03749f8920b7b330ae2e6e41d3f414a,b753b6afd94c77370e97976c023d2729fa586998733fb91e7f28cc4e1c61df444c2640f6ad6369935a800c70372f1b986b525261d0db025290ee03fbf4474050,2013-11-07 18:00:00 UTC",
            "462461d7af9a32000feae87ce851bac230b2f134,79196d52c7dadbc76af237acc709a75cf939ec5097d9ec3f649ec13d1f6a7695efe4f316b705ee09a43c7f45db83c5cc3e05e08bea421b9beb81f7131c988418,2013-11-25 12:45:00 UTC",
            "24f183cf1ed1c21486aa951f1036bfaa46dd1d9c,f26ae0554d3695acf0dea96c2b54df57af02bcb1e11fc4d4c873b828c3f35d14e29af04ac7cd459f369436f452e7730e5a033c901a6a9c5817c1fd892ba3f743,2013-11-18 02:15:00 UTC"
    };

}
