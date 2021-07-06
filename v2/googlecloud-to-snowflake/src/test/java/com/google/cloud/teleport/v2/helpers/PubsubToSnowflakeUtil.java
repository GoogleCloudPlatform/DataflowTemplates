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
package com.google.cloud.teleport.v2.helpers;

import com.google.cloud.teleport.v2.snowflake.options.PubSubToSnowflakeOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/** Utility class for the {@link PubsubToSnowflakeUtil} class. */
public class PubsubToSnowflakeUtil {
	/**
	 * Helper method to provide mock pieline options.
	 * 
	 * @return mock pipeline options
	 */
    public static PubSubToSnowflakeOptions getMockPubSubToSnowflakeOptions(){
    	PubSubToSnowflakeOptions options = PipelineOptionsFactory.fromArgs().withoutStrictParsing()
                .as(PubSubToSnowflakeOptions.class);
        options.setUsername(ValueProvider.StaticValueProvider.of("dummy"));
        options.setRole(ValueProvider.StaticValueProvider.of("dummy"));
        options.setDatabase(ValueProvider.StaticValueProvider.of("dummy"));
        options.setSchema(ValueProvider.StaticValueProvider.of("dummy"));
        options.setWarehouse(ValueProvider.StaticValueProvider.of("dummy"));
        options.setStagingBucketName(ValueProvider.StaticValueProvider.of("gs://bucket/location/"));
        options.setServerName(ValueProvider.StaticValueProvider.of("dummy.server.snowflakecomputing.com"));
        options.setInputSubscription("dummy");
        options.setTable(ValueProvider.StaticValueProvider.of("dummy"));
        options.setSnowPipe(ValueProvider.StaticValueProvider.of("dummy"));
        options.setStorageIntegrationName(ValueProvider.StaticValueProvider.of("dummy"));
        options.setRawPrivateKey(ValueProvider.StaticValueProvider.of("dummy"));
        options.setPrivateKeyPassphrase(ValueProvider.StaticValueProvider.of("dummy"));
        options.setTokenKMSEncryptionKey("dummy");
        options.setSourceFormat("json");
        options.setTempLocation("gs://bucket-name/temp/");
        options.setStreaming(true);
        return options;
    }
}
