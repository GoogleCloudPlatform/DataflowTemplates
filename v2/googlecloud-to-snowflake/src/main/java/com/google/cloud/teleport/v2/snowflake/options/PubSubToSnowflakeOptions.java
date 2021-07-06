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
package com.google.cloud.teleport.v2.snowflake.options;

import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Description;

/**
 * Provides {@link PubSubToSnowflakeOptions} to read records from a Pub/Sub subscription and 
 * write to Snowflake table.
 */
public interface PubSubToSnowflakeOptions extends ReadSubscriptionOptions,SnowflakePipelineOptions {

    
    @Description(
    		"KMS Encryption Key for decrypting the secrets. The Key should be in the format "
    		+ "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    String getTokenKMSEncryptionKey();
    
    void setTokenKMSEncryptionKey(String keyName);
    
    @Description("csv/json format of the source data ")
    String getSourceFormat();
    
    void setSourceFormat(String format);
    
}