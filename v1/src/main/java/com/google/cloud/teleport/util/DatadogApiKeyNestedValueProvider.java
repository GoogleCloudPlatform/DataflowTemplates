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
package com.google.cloud.teleport.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.templates.common.DatadogApiKeySource;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.EnumUtils;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Returns a apiKey from a valid {@link DatadogApiKeySource}. */
public class DatadogApiKeyNestedValueProvider implements ValueProvider<String>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DatadogApiKeyNestedValueProvider.class);

  private transient volatile String cachedValue;

  private final ValueProvider<String> secretId;
  private final ValueProvider<String> kmsEncryptionKey;
  private final ValueProvider<String> apiKey;
  private final ValueProvider<String> apiKeySource;

  @Override
  public String get() {

    if (cachedValue == null) {

      DatadogApiKeySource finalApiKeySource;
      if (apiKeySource.get() == null) {
        // Since apiKey source was introduced after KMS and plaintext options it may not be present.
        // In that case we attempt to determine the apiKey source based on whether only the plaintext
        // apiKey is present or both encrypted apiKey and KMS Key params are present.
        // Passing a apiKeySource is mandatory if the apiKey is stored in Secret Manager.
        finalApiKeySource = resolveApiKeySource();
      } else {
        finalApiKeySource = EnumUtils.getEnum(DatadogApiKeySource.class, apiKeySource.get());
        checkArgument(
            finalApiKeySource != null,
            "apiKeySource must be one of PLAINTEXT, KMS or SECRET_MANAGER, but found: "
                + apiKeySource);
      }

      cachedValue = getApiKey(finalApiKeySource).get();
    }

    return cachedValue;
  }

  @Override
  public boolean isAccessible() {
    return secretId.isAccessible()
        || (kmsEncryptionKey.isAccessible() && apiKey.isAccessible())
        || apiKey.isAccessible();
  }

  public DatadogApiKeyNestedValueProvider(
      ValueProvider<String> secretId,
      ValueProvider<String> kmsEncryptionKey,
      ValueProvider<String> apiKey,
      ValueProvider<String> apiKeySource) {
    this.secretId = secretId;
    this.kmsEncryptionKey = kmsEncryptionKey;
    this.apiKey = apiKey;
    this.apiKeySource = apiKeySource;
  }

  /**
   * Utility method that attempts to determine if apiKey source is KMS or PLAINTEXT based on user
   * provided parameters. Added for backwards compatibility with the KMS and PLAINTEXT apiKey
   * options.
   *
   * @return {@link DatadogApiKeySource}
   */
  @VisibleForTesting
  protected DatadogApiKeySource resolveApiKeySource() {

    if (apiKeyKmsParamsExist()) {
      LOG.info("apiKeySource set to {}", DatadogApiKeySource.KMS);
      return DatadogApiKeySource.KMS;

    } else if (apiKeyPlaintextParamsExist()) {
      LOG.info("apiKeySource set to {}", DatadogApiKeySource.PLAINTEXT);
      return DatadogApiKeySource.PLAINTEXT;

    } else {
      throw new RuntimeException(
          "Could not resolve apiKeySource from given parameters. Pass in a apiKeySource parameter"
              + " with value one of SECRET_MANAGER, KMS or PLAINTEXT.");
    }
  }

  /**
   * Helper method that checks if the Secret ID for required for retrieving the apiKey from Secret
   * Manager exists.
   *
   * @return true if Secret ID exists or false otherwise
   */
  private boolean apiKeySecretManagerParamsExist() {
    return secretId.get() != null;
  }

  /**
   * Helper method that checks params required for decrypting the apiKey using a KMS Key.
   *
   * @return true if encrypted apiKey and KMS Key exist or false otherwise
   */
  private boolean apiKeyKmsParamsExist() {
    return apiKey.get() != null && kmsEncryptionKey.get() != null;
  }

  /**
   * Helper method that checks if the plaintext apiKey exists.
   *
   * @return true if plaintext apiKey exists or false otherwise
   */
  private boolean apiKeyPlaintextParamsExist() {
    return apiKey.get() != null;
  }

  /**
   * Utility method that retrieves the apiKey from a valid {@link DatadogApiKeySource}.
   *
   * @param apiKeySource {@link DatadogApiKeySource}
   * @return {@link ValueProvider} Datadog HEC apiKey
   */
  @VisibleForTesting
  protected ValueProvider<String> getApiKey(DatadogApiKeySource apiKeySource) {
    switch (apiKeySource) {
      case SECRET_MANAGER:
        checkArgument(
            apiKeySecretManagerParamsExist(),
            "apiKeySecretId is required to retrieve apiKey from Secret Manager");

        LOG.info("Using apiKey secret stored in Secret Manager");
        return new SecretManagerValueProvider(secretId);

      case KMS:
        checkArgument(
            apiKeyKmsParamsExist(),
            "apiKey and apiKeyKmsEncryptionKey are required while decrypting using KMS Key");

        LOG.info("Using KMS Key to decrypt apiKey");
        return new KMSEncryptedNestedValueProvider(apiKey, kmsEncryptionKey);

      case PLAINTEXT:
        checkArgument(apiKeyPlaintextParamsExist(), "apiKey is required for writing events");

        LOG.warn(
            "Using plaintext apiKey. Consider storing the apiKey in Secret Manager or "
                + "pass an encrypted apiKey and a KMS Key to decrypt it");
        return apiKey;

      default:
        throw new RuntimeException(
            "apiKeySource must be one of PLAINTEXT, KMS or SECRET_MANAGER, but found: "
                + apiKeySource);
    }
  }
}
