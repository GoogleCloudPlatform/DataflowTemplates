/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.auto.value.AutoOneOf;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import java.io.Serializable;

/**
 * Different sources employ varying styles for referencing a schema. For instance, a Postgres source
 * uses a database name and namespace, while a Cassandra source uses a cluster name and keyspace. In
 * the below Value class we enclose the schema reference. Note: In most of the cases an interface
 * would be a better choice to a `OneOf` pattern. In this particular case though, it's not possible
 * to have a common minimal interface that encodes getters and setters for properties like nameSpace
 * or ClusterName. If we go that route, we would either have to compriamise on readability (like
 * level1, level2 reference etc), or have a bloated interface. Hence, a oneOf of independent (but
 * conceptually related) classes suits a better pattern to encode this information.
 *
 * @see: <a
 *     heref=https://github.com/google/auto/blob/main/value/userguide/howto.md#-make-a-class-where-only-one-of-its-properties-is-ever-set>autoOneOf</a>
 */
@AutoOneOf(SourceSchemaReference.Kind.class)
public abstract class SourceSchemaReference implements Serializable {

  public enum Kind {
    JDBC,
    CASSANDRA
  };

  public abstract Kind getKind();

  public abstract JdbcSchemaReference jdbc();

  public abstract CassandraSchemaReference cassandra();

  public static SourceSchemaReference ofJdbc(JdbcSchemaReference jdbcSchemaReference) {
    return AutoOneOf_SourceSchemaReference.jdbc(jdbcSchemaReference);
  }

  public static SourceSchemaReference ofCassandra(
      CassandraSchemaReference cassandraSchemaReference) {
    return AutoOneOf_SourceSchemaReference.cassandra(cassandraSchemaReference);
  }

  /**
   * Returns a stable unique name to be used in PTransforms.
   *
   * @return name of the {@link SourceTableReference}
   */
  public String getName() {
    switch (this.getKind()) {
      case JDBC:
        return this.jdbc().getName();
      case CASSANDRA:
        return this.cassandra().getName();
      default:
        throw new IllegalStateException("name not implemented for kind " + this.getKind());
    }
  }
}
