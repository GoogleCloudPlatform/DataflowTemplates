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
package com.google.cloud.teleport.v2.snowflake.util;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DecryptUtil} provides helper methods to decrypt the encrypted
 * string using the encryption key.
 * 
 */
public class DecryptUtil {

	private static final Pattern KEYNAME_PATTERN = Pattern
			.compile("projects/([^/]+)/locations/([a-zA-Z0-9_-]{1,63})/keyRings/"
					+ "[a-zA-Z0-9_-]{1,63}/cryptoKeys/[a-zA-Z0-9_-]{1,63}");

	/** The log to output status messages to. */
	private static final Logger LOG = LoggerFactory.getLogger(DecryptUtil.class);

	/**
	 * Uses the GCP KMS client to decrypt an encrypted value using a KMS key of the
	 * form
	 * projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}.
	 * The encrypted value should be a base64 encrypted string which has been
	 * encrypted using the KMS encrypt API call. See <a href=
	 * "https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">
	 * this KMS API Encrypt Link</a>.
	 * 
	 * @param encryptedValue base64 encrypted string
	 * @param kmsKey encryption key 
	 * @return decrypted value
	 * @throws IOException
	 */

	public static String decryptWithKMS(String encryptedValue, String kmsKey) throws IOException {
		/*
		 * kmsKey should be in the following format:
		 * projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/
		 * {kms_key_name}
		 */

		if (kmsKey == null || encryptedValue.isEmpty()) {
			LOG.info("KMS Key is not specified. Using: " + encryptedValue);
			return encryptedValue;
		} else if (!validateKmsKey(kmsKey)) {
			IllegalArgumentException exception = new IllegalArgumentException("Provided KMS Key %s is invalid");
			throw new RuntimeException(exception);
		} else {

			byte[] cipherText = Base64.getDecoder().decode(encryptedValue.getBytes("UTF-8"));

			try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {

				// Decrypt the ciphertext with Cloud KMS.
				DecryptResponse response = client.decrypt(kmsKey, ByteString.copyFrom(cipherText));

				// Extract the plaintext from the response.
				return new String(response.getPlaintext().toByteArray());
			}
		}
	}

	/**
	 * Validates that kms key should be in the following format:
	 * projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/
	 * {kms_key_name}.
	 * 
	 * @param kmsKey the encryption key
	 * @return true if key matches the pattern above else returns false
	 */
	private static boolean validateKmsKey(String kmsKey) {
		return KEYNAME_PATTERN.matcher(kmsKey).matches();
	}

	/**
	 * Provides the raw value which is wrapped by the ValueProvider if the value is accessible. If the value is 
	 * not accessible at execution time then empty string is returned("").
	 * 
	 * @param <T> any subType of ValueProvider interface
	 * @param t a ValueProvider type which wraps the raw value provided at execution phase
	 * @return raw value wrapped by the ValueProvider type.
	 */
    public static <T extends ValueProvider>String getStringValue(T t){
        if (t.isAccessible()) {
            return (String)t.get();
        }
        return "";
    }
    
    /**
     * Checks if pipeline parameter is encrypted. If pipeline parameter is encrypted, returns the decrypted value 
     * using kms encryption key otherwise returns the raw value as is.
     * 
     * @param pipelineParameter runtime parameter passed to pipeline
     * @param kmsKey the encryption key
     * @return decrypted value if pipeline parameter is encrypted otherwise returns the raw value as is.
     */
    public static String getRawEncrypted(ValueProvider<String> pipelineParameter, String kmsKey) {

    	String pipelineParameterStringValue = getStringValue(pipelineParameter);
    	
    	if(isNullOrEmpty(pipelineParameterStringValue)) {
    		return "";
    	}

    	String decryptedValue;
    	if (!isEncrypted(pipelineParameterStringValue)) {
    		return pipelineParameterStringValue;
    	}
    	else {
    		try {
				decryptedValue=decryptWithKMS(extractEncryptedValue(getStringValue(pipelineParameter)),kmsKey);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
   		return decryptedValue;
    	}
    }

    /**
     * Checks if pipeline parameter is encrypted. If pipeline parameter is encrypted, returns the decrypted value 
     * using kms encryption key otherwise returns the raw value as is.
     * 
     * @param pipelineParameter runtime parameter passed to pipeline
     * @param kmsKey the encryption key
     * @return decrypted value if pipeline parameter is encrypted otherwise returns the raw value as is.
     */
    public static String getRawEncrypted(String pipelineParameter, String kmsKey) {
    	
    	if(isNullOrEmpty(pipelineParameter)) {
    		return "";
    	}
    	
    	String decryptedValue;
    	if (!isEncrypted(pipelineParameter)) {
    		return pipelineParameter;
    	}
    	else {
    		try {
				decryptedValue=decryptWithKMS(extractEncryptedValue(pipelineParameter),kmsKey);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
    		return decryptedValue;
    	}
    }
    
    /**
     * Checks whether the argument is encrypted. Encrypted values should be in the following 
     * format : ${encryptedValue}. The raw values can be passed as is.
     * 
     * @param pipelineParameter to check for encryption.
     * @return true if argument is encrypted
     */
    public static boolean isEncrypted(String pipelineParameter) {
    	String regex="\\$\\{.*?}";
    	return pipelineParameter.matches(regex);
    }

    /**
     * Extracts the encrypted value between the template :${}.
     * 
     * @param pipelineParameter encrypted value of the following format : ${encryptedValue}
     * @return extracted value if the argument is encrypted otherwise return the raw value as is.
     */
    public static String extractEncryptedValue(String pipelineParameter){
    	String pattern="\\$\\{(.*?)}";
    	Matcher m = Pattern.compile(pattern).matcher(pipelineParameter);
    	while (m.find()) {
    		return m.group(1);
    	}
    	
    	return "";
    }

    /**
     * Checks if string argument is null or empty.
     * 
     * @param str the string to check
     * @return true if string is null or empty else returns false.
     */
    public static boolean isNullOrEmpty(String str) {
    	return !(str != null && !str.trim().isEmpty());
    }
	
}
