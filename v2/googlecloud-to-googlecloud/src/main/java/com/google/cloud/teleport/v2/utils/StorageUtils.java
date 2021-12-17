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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A set of helper functions and classes for Cloud Storage. */
public class StorageUtils {
  private static final Pattern BUCKET_URN_SPEC = Pattern.compile("^projects/.*/buckets/(.+)$");

  /**
   * Parses a Cloud Storage bucket URN and returns the corresponding bucket name.
   *
   * @param bucketUrn bucket URN in {@code projects/[project_id]/buckets/[bucket_id]} format
   * @return the bucket name extracted from the URN, i.e. {@code [bucket_id]}
   * @throws IllegalArgumentException if the bucket URN format is invalid
   */
  public static String parseBucketUrn(String bucketUrn) {
    Matcher match = BUCKET_URN_SPEC.matcher(bucketUrn);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "Bucket reference is not in projects/[project_id]/buckets/[bucket_id] format: \"%s\"",
              bucketUrn));
    }
    return match.group(1);
  }
}
