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
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


/**
 * The {@link PubSubToSnowflake} is a Streaming pipeline which ingests CSV format messages
 * from Pub/Sub subscription and outputs them into a Snowflake table.
 *
 */
public class PubSubToSnowflake {

	  
	/**
	   * Runs the pipeline to completion with the specified options.
	   * 
	   * @param pubSubToSnowflakeOptions the execution options.
	   * @return the pipeline result.
	 * @throws IOException 
	   **/	
	
	
    public static PipelineResult run(PubSubToSnowflakeOptions pubSubToSnowflakeOptions) throws IOException {
        
    	DecryptUtil decryptUtil = new DecryptUtil(pubSubToSnowflakeOptions.getTokenKMSEncryptionKey());
    	
    	String inputSubscription = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getInputSubscription());      
        String table = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getTable());
        String storageIntegrationName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStorageIntegrationName());
        String stagingBucketName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getStagingBucketName());
        String snowPipe = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSnowPipe());
        String sourceFormat = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSourceFormat());
    	
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
                                .withUserDataMapper(getUserDataMapper(sourceFormat))
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
    	
        String userName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getUsername());
        String password = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getPassword());
        String oauthToken = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getOauthToken());
        String rawPrivateKey = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getRawPrivateKey());
        String privateKeyPassphrase = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getPrivateKeyPassphrase());
        String role = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getRole());
        String serverName = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getServerName());
        String database = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getDatabase());
        String warehouse = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getWarehouse());
        String schema = decryptUtil.decryptIfRequired(pubSubToSnowflakeOptions.getSchema());
                
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
