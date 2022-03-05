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
package com.google.cloud.teleport.v2.snowflake.utils;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DecryptUtil} provides helper methods to decrypt the encrypted
 * string using the encryption key.
 *
 */
public class DecryptUtil {

	private static final Pattern KEY_NAME_PATTERN = Pattern.compile("projects/([^/]+)/locations/([a-zA-Z0-9_-]{1,63})/keyRings/[a-zA-Z0-9_-]{1,63}/cryptoKeys/[a-zA-Z0-9_-]{1,63}");

	private static final Pattern ENCRYPT_PATTERN = Pattern.compile("\\$\\{(.*?)}");

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

    private String kmsKey;
    private KeyManagementServiceClient keyManagementServiceClient;

    public DecryptUtil(String kmsKey) throws IOException {
        if (!Strings.isNullOrEmpty(kmsKey)) {
			this.kmsKey = kmsKey;
			this.keyManagementServiceClient = KeyManagementServiceClient.create();
        }
    }


	private String decryptWithKMS(String encryptedValue) throws IOException {

		if(Strings.isNullOrEmpty(this.kmsKey)){
			return encryptedValue;

		} else {
				byte[] cipherText = Base64.getDecoder().decode(encryptedValue.getBytes("UTF-8"));

				// Decrypt the ciphertext with Cloud KMS.
				DecryptResponse decryptResponse = this.keyManagementServiceClient.decrypt(this.kmsKey, ByteString.copyFrom(cipherText));

				// Extract the plaintext from the response.
				return new String(decryptResponse.getPlaintext().toByteArray());
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
		return KEY_NAME_PATTERN.matcher(kmsKey).matches();
	}

    /**
     * Checks if pipeline parameter is encrypted. If pipeline parameter is
     * encrypted, returns the decrypted value using kms encryption key otherwise
     * returns the raw value as is.
     *
     * @param pipelineOption runtime parameter passed to pipeline
     * @return decrypted value if pipeline parameter is encrypted otherwise returns
     * the raw value as is.
     */
    public ValueProvider<String> decryptIfRequired(ValueProvider<String> pipelineOption) throws IOException {
        String returnValue = null;
        if (pipelineOption.isAccessible() && !Strings.isNullOrEmpty(pipelineOption.get())) {
            String pipelineOptionValue = pipelineOption.get();
            Matcher match = ENCRYPT_PATTERN.matcher(pipelineOptionValue);
            if (match.find()) {
                if (Strings.isNullOrEmpty(this.kmsKey)) {
					throw new IllegalArgumentException(String.format("KMS Key is required when encrypted values are provided for parameters. KMS key should be in the format: %s", KEY_NAME_PATTERN));
				}
				else if(!validateKmsKey(kmsKey)){
					throw new IllegalArgumentException(String.format("Provided KMS Key %s is invalid. KMS key should be in the format: %s", kmsKey, KEY_NAME_PATTERN));
                } else {
                    returnValue = decryptWithKMS(match.group(1));
                }
            } else {
                returnValue = pipelineOptionValue;
            }
        }
        return StaticValueProvider.of(returnValue);
    }
	/**
	 * Checks if pipeline parameter is encrypted. If pipeline parameter is
	 * encrypted, returns the decrypted value using kms encryption key otherwise
	 * returns the raw value as is.
	 *
	 * @param pipelineOption runtime parameter passed to pipeline
	 * @return decrypted value if pipeline parameter is encrypted otherwise returns
	 *         the raw value as is.
	 */
	public ValueProvider<String> decryptIfRequired(String pipelineOption) throws IOException {
		return decryptIfRequired(StaticValueProvider.of(pipelineOption));
	}

	/**
	 * Checks whether the argument is encrypted. Encrypted values should be in the
	 * following format : ${encryptedValue}. The raw values can be passed as is.
	 *
	 * @param pipelineParameter to check for encryption.
	 * @return true if argument is encrypted
	 */
	public static boolean isEncrypted(String pipelineParameter) {
		return ENCRYPT_PATTERN.matcher(pipelineParameter).matches();
	}

	/**
	 * Extracts the encrypted value between the template :${}.
	 *
	 * @param pipelineParameter encrypted value of the following format :
	 *                          ${encryptedValue}
	 * @return extracted value if the argument is encrypted otherwise return the raw
	 *         value as is.
	 */
	public static String extractEncryptedValue(String pipelineParameter) {
		Matcher m = ENCRYPT_PATTERN.matcher(pipelineParameter);
		while (m.find()) {
			return m.group(1);
		}
		return "";
	}

}
