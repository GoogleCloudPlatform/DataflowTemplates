/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to map database character set names to Java {@link Charset} objects. */
public final class CharsetMapper {
  private static final Logger logger = LoggerFactory.getLogger(CharsetMapper.class);

  private static final Map<String, Charset> DB_TO_JAVA_CHARSET = new HashMap<>();

  static {
    // MySQL specific mappings
    DB_TO_JAVA_CHARSET.put("utf8mb4", StandardCharsets.UTF_8);
    DB_TO_JAVA_CHARSET.put("utf8", StandardCharsets.UTF_8);
    DB_TO_JAVA_CHARSET.put("latin1", StandardCharsets.ISO_8859_1);
    DB_TO_JAVA_CHARSET.put("ascii", StandardCharsets.US_ASCII);
    DB_TO_JAVA_CHARSET.put(
        "binary", StandardCharsets.ISO_8859_1); // Map binary to ISO-8859-1 for byte processing

    // PostgreSQL specific mappings
    DB_TO_JAVA_CHARSET.put("utf8", StandardCharsets.UTF_8);
    DB_TO_JAVA_CHARSET.put("sql_ascii", StandardCharsets.US_ASCII);
    DB_TO_JAVA_CHARSET.put("latin1", StandardCharsets.ISO_8859_1);
  }

  /**
   * Map a database charset name to a Java {@link Charset}.
   *
   * @param dbCharsetName The database charset name (e.g., "utf8mb4", "latin1").
   * @return An {@link Optional} containing the Java {@link Charset} if supported, or empty.
   */
  public static Optional<Charset> toJavaCharset(String dbCharsetName) {
    if (dbCharsetName == null) {
      return Optional.empty();
    }
    String normalizedName = dbCharsetName.toLowerCase().trim();
    if (normalizedName.isEmpty()) {
      return Optional.empty();
    }

    // 1. Try explicit mapping
    if (DB_TO_JAVA_CHARSET.containsKey(normalizedName)) {
      return Optional.of(DB_TO_JAVA_CHARSET.get(normalizedName));
    }

    // 2. Try normalization (replace underscores with hyphens, etc.)
    try {
      return Optional.of(Charset.forName(normalizedName));
    } catch (IllegalArgumentException e) {
      // Try replacing _ with -
      String alternateName = normalizedName.replace('_', '-');
      try {
        return Optional.of(Charset.forName(alternateName));
      } catch (IllegalArgumentException ex) {
        logger.warn("Unsupported database charset: {}", dbCharsetName);
        return Optional.empty();
      }
    }
  }

  private CharsetMapper() {}
}
