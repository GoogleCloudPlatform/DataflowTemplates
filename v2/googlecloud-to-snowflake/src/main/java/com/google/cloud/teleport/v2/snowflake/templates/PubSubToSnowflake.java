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
import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link PubSubToSnowflake} is a Streaming pipeline which ingests CSV format messages
 * from Pub/Sub subscription and outputs them into a Snowflake table.
 *
 */
public class PubSubToSnowflake {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToSnowflake.class);

	/**
	   * Runs the pipeline to completion with the specified options.
	   * 
	   * @param pubSubToSnowflakeOptions the execution options.
	   * @return the pipeline result.
	 * @throws IOException 
	   **/	
	
	
    public static PipelineResult run(PubSubToSnowflakeOptions pubSubToSnowflakeOptions) throws IOException {
        
    	DecryptUtil decryptUtil = new DecryptUtil(pubSubToSnowflakeOptions.getTokenKMSEncryptionKey());

        ValueProvider<String> inputSubscription = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getInputSubscription());
        ValueProvider<String> table = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getTable());
        ValueProvider<String> storageIntegrationName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStorageIntegrationName());
        ValueProvider<String> stagingBucketName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStagingBucketName());
        ValueProvider<String> snowPipe = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSnowPipe());
        ValueProvider<String> sourceFormat = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSourceFormat());
    	
        final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = getSnowFlakeDataSourceConfiguration(pubSubToSnowflakeOptions);

        Pipeline pipeline = Pipeline.create(pubSubToSnowflakeOptions);
        pipeline
                .apply("Read PubSub Events",
        		        PubsubIO.readStrings().fromSubscription(inputSubscription))
                .apply("Stream To Snowflake",
                        SnowflakeIO.<String>write()
                                .to(table)
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withStorageIntegrationName(storageIntegrationName)
                                .withStagingBucketName(stagingBucketName)
                                .withUserDataMapper(getUserDataMapper(sourceFormat.get()))
                                .withSnowPipe(snowPipe));

        return pipeline.run();
    }


    /**
     * Builds and returns Snowflake DataSource configuration object based on executor specified options 
     * to support various authentication methods(Key Pair, Username and password & OAuth).
     * 
     * @param pubSubToSnowflakeOptions the execution options.
     * @return the Snowflake DataSource configuration.
     * @throws IOException 
     */
    private static SnowflakeIO.DataSourceConfiguration getSnowFlakeDataSourceConfiguration(PubSubToSnowflakeOptions pubSubToSnowflakeOptions) throws IOException{
    	
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
        SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = null;

        LOG.info("Going to create dataSourceConfiguration");
        if((Strings.isNullOrEmpty(userName.get()) || Strings.isNullOrEmpty(password.get()) || Strings.isNullOrEmpty(oauthToken.get()) ||
                Strings.isNullOrEmpty(rawPrivateKey.get()) || Strings.isNullOrEmpty(privateKeyPassphrase.get()) ||
                Strings.isNullOrEmpty(role.get()) || Strings.isNullOrEmpty(serverName.get()) || Strings.isNullOrEmpty(database.get()) ||
                Strings.isNullOrEmpty(warehouse.get()) || Strings.isNullOrEmpty(schema.get()))){
            dataSourceConfiguration = SnowflakeIO.DataSourceConfiguration.create();

            if(!Strings.isNullOrEmpty(userName.get()) && !Strings.isNullOrEmpty(rawPrivateKey.get()) &&
                    !Strings.isNullOrEmpty(privateKeyPassphrase.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withKeyPairRawAuth(userName,rawPrivateKey,privateKeyPassphrase);
            }

            if(!Strings.isNullOrEmpty(userName.get()) && !Strings.isNullOrEmpty(password.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withUsernamePasswordAuth(userName,password);
            }

            if(!Strings.isNullOrEmpty(oauthToken.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withOAuth(oauthToken);
            }

            if(!Strings.isNullOrEmpty(role.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withRole(role);
            }

            if(!Strings.isNullOrEmpty(serverName.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withServerName(serverName);
            }

            if(!Strings.isNullOrEmpty(database.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withDatabase(database);
            }

            if(!Strings.isNullOrEmpty(warehouse.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withWarehouse(warehouse);
            }

            if(!Strings.isNullOrEmpty(schema.get())) {
                dataSourceConfiguration = dataSourceConfiguration.withSchema(schema);
            }
        }
        return dataSourceConfiguration;
    }
        
    /**
     * The main entry-point for pipeline execution. This method will start the pipeline.
     * 
     * 
     * @param args command-line args passed by the executor.
     * @throws IOException 
     */
	public static void main(String[] args) throws IOException {
		PubSubToSnowflakeOptions pubSubToSnowflakeOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
				.fromArgs(args).as(PubSubToSnowflakeOptions.class);
		pubSubToSnowflakeOptions.setStreaming(true);
		run(pubSubToSnowflakeOptions);
	}

}
