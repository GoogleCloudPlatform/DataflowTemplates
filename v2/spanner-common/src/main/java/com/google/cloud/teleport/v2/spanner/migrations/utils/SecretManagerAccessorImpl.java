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
  private static final Pattern partialPattern = Pattern.compile("projects/.*/secrets/.*");
  private static final Pattern fullPattern = Pattern.compile("projects/.*/secrets/.*/versions/.*");
  private static final Pattern partialWithSlash = Pattern.compile("projects/.*/secrets/.*/");

  public String getSecret(String secretName) {
    return SecretManagerUtils.getSecret(secretName);
  }

  public String resolvePassword(String secretManagerUri, String logicalShardId, String password) {
    if (secretManagerUri != null && !secretManagerUri.isEmpty()) {
      LOG.info(
          "Secret Manager will be used to get password for shard {} having secret {}",
          logicalShardId,
          secretManagerUri);
      if (partialPattern.matcher(secretManagerUri).matches()) {
        LOG.info("The matched secret for shard {} is : {}", logicalShardId, secretManagerUri);
        if (fullPattern.matcher(secretManagerUri).matches()) {
          LOG.info("The secret for shard {} is : {}", logicalShardId, secretManagerUri);
        } else {
          // partial match hence get the latest version
          String versionToAppend = "versions/latest";
          if (partialWithSlash.matcher(secretManagerUri).matches()) {
            secretManagerUri += versionToAppend;
          } else {
            secretManagerUri += "/" + versionToAppend;
          }

          LOG.info("The generated secret for shard {} is : {}", logicalShardId, secretManagerUri);
        }
        return getSecret(secretManagerUri);
      } else {
        LOG.error(
            "The secretManagerUri field with value {} for shard {} , specified in source config file."
                + " not adhere to expected pattern projects/.*/secrets/.*/versions/.*",
            secretManagerUri,
            logicalShardId);
        throw new RuntimeException(
            "The secretManagerUri field with value "
                + secretManagerUri
                + " for shard "
                + logicalShardId
                + ", specified in source config file"
                + " does not adhere to expected pattern"
                + " projects/.*/secrets/.*/versions/.*");
      }
    }
    LOG.info("using plaintext password for shard: {}", logicalShardId);
    return password;
  }
}
