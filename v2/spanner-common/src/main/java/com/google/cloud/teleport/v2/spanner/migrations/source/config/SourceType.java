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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

/** Enum representing the supported migration source database types. */
public enum SourceType {
  /** Apache Cassandra source database type. */
  CASSANDRA(new String[] {"cassandra"}),

  /** Astra DB source database type. */
  ASTRA_DB(new String[] {"astra_db"}),

  /** MySQL source database type. */
  MYSQL(new String[] {"mysql"}),

  /** PostgreSQL source database type. */
  PG(new String[] {"postgresql"});

  private final String[] sourceTypeStringValues;

  SourceType(String[] sourceTypeStringValues) {
    this.sourceTypeStringValues = sourceTypeStringValues;
  }

  /**
   * Parses a string representation of a source type and returns the corresponding {@link
   * SourceType}.
   *
   * @param sourceTypeStringValue the string value representing the source type (case-insensitive)
   * @return the matching {@link SourceType}
   * @throws IllegalArgumentException if the source type string is not supported
   */
  public static SourceType parseSourceType(String sourceTypeStringValue) {
    sourceTypeStringValue = sourceTypeStringValue.toLowerCase();
    for (SourceType sourceType : SourceType.values()) {
      for (String value : sourceType.sourceTypeStringValues) {
        if (value.equals(sourceTypeStringValue)) {
          return sourceType;
        }
      }
    }
    throw new IllegalArgumentException("Unsupported source type: " + sourceTypeStringValue);
  }
}
