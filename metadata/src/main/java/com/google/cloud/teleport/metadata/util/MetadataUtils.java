/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.metadata.util;

/** Utilities for working with template metadata. */
public final class MetadataUtils {

  private MetadataUtils() {}

  /**
   * There are cases in which users will pass a gs://{bucketName} or a gs://{bucketName}/path
   * wrongly to a bucket name property. This will ensure that execution will run as expected
   * considering some input variations.
   *
   * @param bucketName User input with the bucket name.
   * @return Bucket name if parseable, or throw exception otherwise.
   * @throws IllegalArgumentException If bucket name can not be handled as such.
   */
  public static String bucketNameOnly(String bucketName) {

    String changedName = bucketName;
    // replace leading gs://
    if (changedName.startsWith("gs://")) {
      changedName = changedName.replaceFirst("gs://", "");
    }
    // replace trailing slash
    if (changedName.endsWith("/")) {
      changedName = changedName.replaceAll("/$", "");
    }

    if (changedName.contains("/") || changedName.contains(":")) {
      throw new IllegalArgumentException(
          "Bucket name "
              + bucketName
              + " is invalid. It should only contain the name of the bucket (not a path or URL).");
    }

    return changedName;
  }
}
