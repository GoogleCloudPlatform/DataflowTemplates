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
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class to safely generate valid single-character codepoints for a given {@link Charset}.
 */
public final class CodepointGenerator {

  /**
   * Generate all valid single-character Unicode codepoints for the given Charset, up to a maximum
   * limit.
   *
   * <p>If the number of valid codepoints exceeds the limit, it returns an empty Optional to
   * indicate that the charset is too large for Java-side generation and should fall back to
   * database-side SQL.
   *
   * @param charset The target {@link Charset}.
   * @param maxLimit The maximum number of codepoints allowed before aborting.
   * @return An {@link Optional} containing the list of codepoints, or empty if the limit was
   *     exceeded.
   */
  public static Optional<List<Integer>> getValidCodepoints(Charset charset, int maxLimit) {
    // Optimize: UTF-8 and UTF-16 are known to be very large, abort early.
    String name = charset.name().toUpperCase();
    if (name.contains("UTF-8") || name.contains("UTF-16") || name.contains("UTF32")) {
      return Optional.empty();
    }

    List<Integer> validCodepoints = new ArrayList<>();
    CharsetEncoder encoder = charset.newEncoder();

    // Iterate through all Unicode codepoints.
    // We skip surrogate pairs (U+D800 to U+DFFF) as they are not valid standalone characters.
    for (int cp = 0; cp <= Character.MAX_CODE_POINT; cp++) {
      if (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE) {
        continue;
      }
      if (Character.isDefined(cp)) {
        String s = new String(Character.toChars(cp));
        if (encoder.canEncode(s)) {
          validCodepoints.add(cp);
          if (validCodepoints.size() > maxLimit) {
            return Optional.empty(); // Exceeded limit, abort and use fallback
          }
        }
      }
    }
    return Optional.of(validCodepoints);
  }

  private CodepointGenerator() {}
}
