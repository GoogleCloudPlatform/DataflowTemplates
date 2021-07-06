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

import static com.google.cloud.teleport.v2.snowflake.util.DecryptUtil.getRawEncrypted;
import static com.google.cloud.teleport.v2.snowflake.util.SimpleMapper.getCsvUserDataMapper;

import com.google.cloud.teleport.v2.snowflake.options.PubSubToSnowflakeOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The {@link PubsubToSnowflake} is a Stream pipeline which ingests CSV format messages
 * from Pub/Sub subscription and outputs them into a Snowflake table.
 *
 */
public class PubSubToSnowflake {

	  /**
	   * Runs the pipeline to completion with the specified options.
	   * 
	   * @param options the execution options.
	   * @return the pipeline result.
	   **/	
    public static PipelineResult run(PubSubToSnowflakeOptions options) {
        
        String inputSubscription = getRawEncrypted(options.getInputSubscription(),options.getTokenKMSEncryptionKey());      
        String table = getRawEncrypted(options.getTable(),options.getTokenKMSEncryptionKey());
        String storageIntegrationName = getRawEncrypted(options.getStorageIntegrationName(),options.getTokenKMSEncryptionKey());
        String stagingBucketName = getRawEncrypted(options.getStagingBucketName(),options.getTokenKMSEncryptionKey());
        String snowPipe = getRawEncrypted(options.getSnowPipe(),options.getTokenKMSEncryptionKey());
        String sourceFormat = getRawEncrypted(options.getSourceFormat(),options.getTokenKMSEncryptionKey());
    	
        

        final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration = getSnowFlakeDataSourceConfiguration(options);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read PubSub Events",
        		        PubsubIO.readStrings().fromSubscription(inputSubscription))
                .apply("Stream To Snowflake",
                        SnowflakeIO.<String>write()
                                .to(table)
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withStorageIntegrationName(storageIntegrationName)
                                .withStagingBucketName(stagingBucketName)
                                .withUserDataMapper(getCsvUserDataMapper(sourceFormat))
                                .withSnowPipe(snowPipe));

        return pipeline.run();
    }


    /**
     * Builds and returns Snowflake DataSource configuration object based on executor specified options 
     * to support various authentication methods(Key Pair, Username and password & OAuth).
     * 
     * @param options the execution options.
     * @return the Snowflake DataSource configuration.
     */
    private static SnowflakeIO.DataSourceConfiguration getSnowFlakeDataSourceConfiguration(PubSubToSnowflakeOptions options){
    	
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
        
    /**
     * The main entry-point for pipeline execution. This method will start the pipeline.
     * 
     * 
     * @param args command-line args passed by the executor.
     */
    public static void main(String[] args) {
    	PubSubToSnowflakeOptions options = PipelineOptionsFactory.fromArgs().withoutStrictParsing()
                .fromArgs(args)
                .as(PubSubToSnowflakeOptions.class);
        options.setStreaming(true);
        run(options);
    }
    
}

