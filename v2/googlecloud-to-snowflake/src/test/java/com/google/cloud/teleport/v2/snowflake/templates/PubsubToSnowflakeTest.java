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
package com.google.cloud.teleport.v2.snowflake.templates;


import static com.google.cloud.teleport.v2.snowflake.util.SimpleMapper.getUserDataMapper;

import com.google.cloud.teleport.v2.snowflake.options.PubSubToSnowflakeOptions;
import com.google.cloud.teleport.v2.snowflake.util.DecryptUtil;
import com.google.cloud.teleport.v2.snowflake.utils.PubsubToSnowflakeUtil;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
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
public class PubsubToSnowflakeTest implements Serializable  {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static transient List<String> testMessages;

    private static PubSubToSnowflakeOptions pubSubToSnowflakeOptions;

    public PubsubToSnowflakeTest(){
        this.pubSubToSnowflakeOptions = PubsubToSnowflakeUtil.getMockPubSubToSnowflakeOptions();
    }
   

    /** Tests the execution of pipeline with specified options. 
     * @throws IOException */
    @Test
    public void initTest() throws IOException {
    	 
         PipelineOptionsFactory.register(PubSubToSnowflakeOptions.class);
         PubSubToSnowflakeOptions pubSubToSnowflakeOptions = this.pubSubToSnowflakeOptions;
         final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = getMockSnowflakeConfig(pubSubToSnowflakeOptions);
         
         DecryptUtil decryptUtil= new DecryptUtil(pubSubToSnowflakeOptions.getTokenKMSEncryptionKey());

         ValueProvider<String> table = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getTable());
         ValueProvider<String> storageIntegrationName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStorageIntegrationName());
         ValueProvider<String> stagingBucketName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStagingBucketName());
         ValueProvider<String> snowPipe = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSnowPipe());
         ValueProvider<String> sourceFormat = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSourceFormat());
         
         Pipeline pipeline = Pipeline.create(pubSubToSnowflakeOptions);
         pipeline
                .apply("Read PubSub Events as Strings",
                        Create.of(testMessages))
                .apply("Stream To Snowflake",
                        SnowflakeIO.<String>write()
                                .to(table)
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withStorageIntegrationName(storageIntegrationName)
                                .withStagingBucketName(stagingBucketName)
                                .withUserDataMapper(getUserDataMapper(sourceFormat.get()))
                                .withSnowPipe(snowPipe));

        PipelineResult state = pipeline.create(pubSubToSnowflakeOptions).run();
        Assert.assertTrue(state.getState().equals(PipelineResult.State.DONE));

    }

    /** Helper method to get mock Snowflake Datasource configuration. 
     * @throws IOException */
    public SnowflakeIO.DataSourceConfiguration getMockSnowflakeConfig(PubSubToSnowflakeOptions pubSubToSnowflakeOptions) throws IOException {

    	DecryptUtil decryptUtil= new DecryptUtil(pubSubToSnowflakeOptions.getTokenKMSEncryptionKey());

    	ValueProvider<String> userName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getUsername());
        ValueProvider<String> password = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getPassword());
        ValueProvider<String> oauthToken = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getOauthToken());
        ValueProvider<String> rawPrivateKey = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getRawPrivateKey());
        ValueProvider<String> privateKeyPassphrase = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getPrivateKeyPassphrase());
        ValueProvider<String> role = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getRole());
        ValueProvider<String> serverName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getServerName());
        ValueProvider<String> database = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getDatabase());
        ValueProvider<String> warehouse = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getWarehouse());
        ValueProvider<String> schema = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSchema());

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
            "55d,b753,2013-11-07 18:00:00, 2015-11-07 18:00:00",
            "462,7919,2013-11-25 12:45:00, 2015-11-07 13:00:00",
            "24f,f26a,2013-11-18 02:15:00, 2015-11-07 19:00:00"
    };

}
