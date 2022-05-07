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

import com.google.cloud.teleport.templates.common.SplunkTokenSource;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.EnumUtils;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Returns a token from a valid {@link SplunkTokenSource}. */
public class TokenNestedValueProvider implements ValueProvider<String>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TokenNestedValueProvider.class);

  private transient volatile String cachedValue;

  private final ValueProvider<String> secretId;
  private final ValueProvider<String> kmsEncryptionKey;
  private final ValueProvider<String> token;
  private final ValueProvider<String> tokenSource;

  @Override
  public String get() {

    if (cachedValue == null) {

      SplunkTokenSource finalTokenSource;
      if (tokenSource.get() == null) {
        // Since token source was introduced after KMS and plaintext options it may not be present.
        // In that case we attempt to determine the token source based on whether only the plaintext
        // token is present or both encrypted token and KMS Key params are present.
        // Passing a tokenSource is mandatory if the token is stored in Secret Manager.
        finalTokenSource = resolveTokenSource();
      } else {
        finalTokenSource = EnumUtils.getEnum(SplunkTokenSource.class, tokenSource.get());
        checkArgument(
            finalTokenSource != null,
            "tokenSource must be one of PLAINTEXT, KMS or SECRET_MANAGER, but found: "
                + tokenSource);
      }

      cachedValue = getToken(finalTokenSource).get();
    }

    return cachedValue;
  }

  @Override
  public boolean isAccessible() {
    return secretId.isAccessible()
        || (kmsEncryptionKey.isAccessible() && token.isAccessible())
        || token.isAccessible();
  }

  public TokenNestedValueProvider(
      ValueProvider<String> secretId,
      ValueProvider<String> kmsEncryptionKey,
      ValueProvider<String> token,
      ValueProvider<String> tokenSource) {
    this.secretId = secretId;
    this.kmsEncryptionKey = kmsEncryptionKey;
    this.token = token;
    this.tokenSource = tokenSource;
  }

  /**
   * Utility method that attempts to determine if token source is KMS or PLAINTEXT based on user
   * provided parameters. Added for backwards compatibility with the KMS and PLAINTEXT token
   * options.
   *
   * @return {@link SplunkTokenSource}
   */
  @VisibleForTesting
  protected SplunkTokenSource resolveTokenSource() {

    if (tokenKmsParamsExist()) {
      LOG.info("tokenSource set to {}", SplunkTokenSource.KMS);
      return SplunkTokenSource.KMS;

    } else if (tokenPlaintextParamsExist()) {
      LOG.info("tokenSource set to {}", SplunkTokenSource.PLAINTEXT);
      return SplunkTokenSource.PLAINTEXT;

    } else {
      throw new RuntimeException(
          "Could not resolve tokenSource from given parameters. Pass in a tokenSource parameter"
              + " with value one of SECRET_MANAGER, KMS or PLAINTEXT.");
    }
  }

  /**
   * Helper method that checks if the Secret ID for required for retrieving the token from Secret
   * Manager exists.
   *
   * @return true if Secret ID exists or false otherwise
   */
  private boolean tokenSecretManagerParamsExist() {
    return secretId.get() != null;
  }

  /**
   * Helper method that checks params required for decrypting the token using a KMS Key.
   *
   * @return true if encrypted token and KMS Key exist or false otherwise
   */
  private boolean tokenKmsParamsExist() {
    return token.get() != null && kmsEncryptionKey.get() != null;
  }

  /**
   * Helper method that checks if the plaintext token exists.
   *
   * @return true if plaintext token exists or false otherwise
   */
  private boolean tokenPlaintextParamsExist() {
    return token.get() != null;
  }

  /**
   * Utility method that retrieves the token from a valid {@link SplunkTokenSource}.
   *
   * @param tokenSource {@link SplunkTokenSource}
   * @return {@link ValueProvider} Splunk HEC token
   */
  @VisibleForTesting
  protected ValueProvider<String> getToken(SplunkTokenSource tokenSource) {
    switch (tokenSource) {
      case SECRET_MANAGER:
        checkArgument(
            tokenSecretManagerParamsExist(),
            "tokenSecretId is required to retrieve token from Secret Manager");

        LOG.info("Using token secret stored in Secret Manager");
        return new SecretManagerValueProvider(secretId);

      case KMS:
        checkArgument(
            tokenKmsParamsExist(),
            "token and tokenKmsEncryptionKey are required while decrypting using KMS Key");

        LOG.info("Using KMS Key to decrypt token");
        return new KMSEncryptedNestedValueProvider(token, kmsEncryptionKey);

      case PLAINTEXT:
        checkArgument(tokenPlaintextParamsExist(), "token is required for writing events");

        LOG.warn(
            "Using plaintext token. Consider storing the token in Secret Manager or "
                + "pass an encrypted token and a KMS Key to decrypt it");
        return token;

      default:
        throw new RuntimeException(
            "tokenSource must be one of PLAINTEXT, KMS or SECRET_MANAGER, but found: "
                + tokenSource);
    }
  }
}
