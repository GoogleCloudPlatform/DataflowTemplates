/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.spanner.common;

import com.google.cloud.spanner.Dialect;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Cloud Spanner Ddl utility components.
 */
public class DdlUtils {

  // Private constructor to prevent initializing instance, because this class is only served within
  // ddl directory
  private DdlUtils() {
  }

  // Shared at package-level
  public static final Escaper OPTION_STRING_ESCAPER =
      Escapers.builder()
          .addEscape('"', "\\\"")
          .addEscape('\\', "\\\\")
          .addEscape('\r', "\\r")
          .addEscape('\n', "\\n")
          .build();
  private static final String POSTGRESQL_IDENTIFIER_QUOTE = "\"";
  private static final String GSQL_IDENTIFIER_QUOTE = "`";
  public static final String POSTGRESQL_LITERAL_QUOTE = "'";
  public static final String GSQL_LITERAL_QUOTE = "\"";

  public static String quoteIdentifier(String name, Dialect dialect) {
    String quote = identifierQuote(dialect);
    return Arrays.stream(name.split("\\.")).map(s -> quote + s + quote)
        .collect(Collectors.joining("."));
  }

  private static String identifierQuote(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return POSTGRESQL_IDENTIFIER_QUOTE;
      case GOOGLE_STANDARD_SQL:
        return GSQL_IDENTIFIER_QUOTE;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized dialect: %s", dialect));
    }
  }

  public static String literalQuote(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return POSTGRESQL_LITERAL_QUOTE;
      case GOOGLE_STANDARD_SQL:
        return GSQL_LITERAL_QUOTE;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized dialect: %s", dialect));
    }
  }
}
