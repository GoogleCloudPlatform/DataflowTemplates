/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.util;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Pattern;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KMSEncryptedNestedValueProvider} is a subclass of {@link DualInputNestedValueProvider}
 * that allows for taking two {@link ValueProvider} objects - one as an encrypted string and the
 * other as a KMS encryption key. If no encryption key is passed, the string is returned, else the
 * encryption key is used to decrypt the encrypted string.
 */
public class KMSEncryptedNestedValueProvider
    extends DualInputNestedValueProvider<String, String, String> {
  private static final Pattern KEYNAME_PATTERN =
      Pattern.compile(
          "projects/([^/]+)/locations/([a-zA-Z0-9_-]{1,63})/keyRings/"
              + "[a-zA-Z0-9_-]{1,63}/cryptoKeys/[a-zA-Z0-9_-]{1,63}");

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(KMSEncryptedNestedValueProvider.class);

  private static class KmsTranslatorInput
      implements SerializableFunction<TranslatorInput<String, String>, String> {
    private KmsTranslatorInput() {}

    public static KmsTranslatorInput of() {
      return new KmsTranslatorInput();
    }

    @Override
    public String apply(TranslatorInput<String, String> input) {
      String decrypted;
      String unencrypted;
      String kmsKey;

      unencrypted = input.getX();
      kmsKey = input.getY();

      if (kmsKey == null) {

        if (unencrypted == null) {
          LOG.warn("KMS Key is not specified and unencrypted value not given.");
          return null;
        }

        LOG.info(
            "KMS Key is not specified. Using unencrypted value of {} bytes",
            unencrypted.getBytes(StandardCharsets.UTF_8).length);
        return unencrypted;
      } else if (!checkKmsKey(kmsKey)) {
        IllegalArgumentException exception =
            new IllegalArgumentException("Provided KMS Key %s is invalid");
        throw new RuntimeException(exception);
      } else {
        try {
          decrypted = decryptWithKMS(unencrypted /*value*/, kmsKey /*key*/);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return decrypted;
      }
    }
  }

  /**
   * Creates a {@link KMSEncryptedNestedValueProvider} that wraps the key and the encrypted value.
   */
  public KMSEncryptedNestedValueProvider(ValueProvider<String> value, ValueProvider<String> key) {
    super(value, key, KmsTranslatorInput.of());
  }

  private static boolean checkKmsKey(String kmsKey) {
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
    kmsKey should be in the following format:
    projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}
     */

    byte[] cipherText = Base64.getDecoder().decode(encryptedValue.getBytes("UTF-8"));

    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {

      // Decrypt the ciphertext with Cloud KMS.
      DecryptResponse response = client.decrypt(kmsKey, ByteString.copyFrom(cipherText));

      // Extract the plaintext from the response.
      return new String(response.getPlaintext().toByteArray());
    }
  }
}
