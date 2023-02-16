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
package com.google.cloud.teleport.it.cassandra;

import static com.google.cloud.teleport.it.utils.ResourceManagerUtils.generateResourceId;

import com.google.re2j.Pattern;
import java.time.format.DateTimeFormatter;

/** Utilities for {@link CassandraResourceManager} implementations. */
final class CassandraResourceManagerUtils {

  private static final int MAX_DATABASE_NAME_LENGTH = 63;
  private static final Pattern ILLEGAL_DATABASE_NAME_CHARS =
      Pattern.compile("[\\/\\\\. \"\0$]"); // i.e. [/\. "$]
  private static final String REPLACE_DATABASE_NAME_CHAR = "-";
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

  private CassandraResourceManagerUtils() {}

  /**
   * Generates a Cassandra database name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The database name string.
   */
  static String generateKeyspaceName(String baseString) {
    return generateResourceId(
            baseString,
            ILLEGAL_DATABASE_NAME_CHARS,
            REPLACE_DATABASE_NAME_CHAR,
            MAX_DATABASE_NAME_LENGTH,
            TIME_FORMAT)
        .replace('-', '_');
  }
}
