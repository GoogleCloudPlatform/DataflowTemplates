/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents the Cassandra Adapter Annotations specified in Options. */
@AutoValue
public abstract class CassandraAnnotations implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraAnnotations.class);

  /**
   * Cassandra Type. Passed as `Options (cassandra_type=type)` Empty string if cassandra_type is not
   * set.
   */
  public abstract CassandraType cassandraType();

  /*
   * TODO(Add backing table once supported).
   */

  public static Builder builder() {
    return new AutoValue_CassandraAnnotations.Builder();
  }

  public static CassandraAnnotations fromColumnOptions(
      List<String> columnOptions, String columnName) {
    ImmutableList<String> cassandraTypes =
        columnOptions.stream()
            .map(o -> o.toUpperCase().replaceAll("\\s+", ""))
            .filter(o -> o.contains("CASSANDRA_TYPE="))
            .map(o -> o.replaceAll("CASSANDRA_TYPE=", ""))
            .map(o -> o.replaceAll("['\"]", ""))
            .collect(ImmutableList.toImmutableList());
    String typeName = (cassandraTypes.isEmpty()) ? "" : cassandraTypes.get(0);
    CassandraAnnotations annotation =
        CassandraAnnotations.builder()
            .setCassandraType(CassandraType.fromAnnotation(typeName))
            .build();
    LOG.info(
        "Cassandra Type for column {} is {}, Annotation is {}",
        columnName,
        cassandraTypes,
        annotation);
    return annotation;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setCassandraType(CassandraType cassandraType);

    public abstract CassandraAnnotations build();
  }
}
