/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to decrypt using KMS key. */
public class KMSUtils {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(KMSUtils.class);
  private static final Pattern KEYNAME_PATTERN =
      Pattern.compile(
          "projects/([^/]+)/locations/([a-zA-Z0-9_-]{1,63})/keyRings/"
              + "[a-zA-Z0-9_-]{1,63}/cryptoKeys/[a-zA-Z0-9_-]{1,63}");

  public static String maybeDecrypt(String unencrypted, String kmsKey) {
    if (kmsKey == null || kmsKey.isEmpty()) {
      LOG.info("KMS Key is not specified. Not decrypting.");
      return unencrypted;
    } else if (!isKmsKey(kmsKey)) {
      IllegalArgumentException exception =
          new IllegalArgumentException("Provided KMS Key %s is invalid");
      throw new RuntimeException(exception);
    } else {
      String decrypted;
      try {
        decrypted = decryptWithKMS(unencrypted /*value*/, kmsKey /*key*/);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return decrypted;
    }
  }

  private static boolean isKmsKey(String kmsKey) {
    return KEYNAME_PATTERN.matcher(kmsKey).matches();
  }

  /**
   * Uses the GCP KMS client to decrypt an encrypted value using a KMS key of the form
   * projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name} The
   * encrypted value should be a base64 encrypted string which has been encrypted using the KMS
   * encrypt API call. See <a
   * href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">
   * this KMS API Encrypt Link</a>.
   */
  private static String decryptWithKMS(String encryptedValue, String kmsKey) throws IOException {
    /*
     * kmsKey should be in the following format:
     * projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}
     */
    byte[] cipherText = Base64.getDecoder().decode(encryptedValue.getBytes(StandardCharsets.UTF_8));
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      // Decrypt the ciphertext with Cloud KMS.
      DecryptResponse response = client.decrypt(kmsKey, ByteString.copyFrom(cipherText));
      // Extract the plaintext from the response.
      return new String(response.getPlaintext().toByteArray());
    }
  }
}
