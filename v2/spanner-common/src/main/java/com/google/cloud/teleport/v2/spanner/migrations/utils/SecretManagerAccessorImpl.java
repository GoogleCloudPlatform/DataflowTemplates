/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation to access secret from Secret Manager. */
public class SecretManagerAccessorImpl implements ISecretManagerAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerAccessorImpl.class);
  private static final Pattern PARTIAL_PATTERN = Pattern.compile("projects/.*/secrets/.*");
  private static final Pattern FULL_PATTERN = Pattern.compile("projects/.*/secrets/.*/versions/.*");
  private static final Pattern PARTIAL_WITH_SLASH = Pattern.compile("projects/.*/secrets/.*/");

  public String getSecret(String secretName) {
    return SecretManagerUtils.getSecret(secretName);
  }

  public String resolvePassword(String secretManagerUri, String logicalShardId, String password) {
    if (secretManagerUri != null && !secretManagerUri.isEmpty()) {
      if (PARTIAL_PATTERN.matcher(secretManagerUri).matches()) {
        if (!FULL_PATTERN.matcher(secretManagerUri).matches()) {
          // partial match hence get the latest version
          String versionToAppend = "versions/latest";
          if (PARTIAL_WITH_SLASH.matcher(secretManagerUri).matches()) {
            secretManagerUri += versionToAppend;
          } else {
            secretManagerUri += "/" + versionToAppend;
          }
        }
        LOG.info("The generated secret for shard {} is : {}", logicalShardId, secretManagerUri);
        return getSecret(secretManagerUri);
      } else {
        String errorMessage =
            "The secretManagerUri field with value "
                + secretManagerUri
                + " for shard "
                + logicalShardId
                + ", specified in source config file"
                + " does not adhere to expected pattern"
                + " projects/{project}/secrets/{secret}/versions/{version}.";
        LOG.error(errorMessage);
        throw new RuntimeException(errorMessage);
      }
    }
    LOG.info("using plaintext password for shard: {}", logicalShardId);
    return password;
  }
}
