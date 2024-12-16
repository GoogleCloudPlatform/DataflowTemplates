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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.schema;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
import java.io.Serializable;
import javax.annotation.Nullable;

@AutoValue
public abstract class CassandraSchemaReference implements Serializable {

  /**
   * Name of the Cassandra KeySpace. This is equivalent of the JDBC database name.
   *
   * <p>Note that Cassandra also has a clusterName, which is at the level of MySQL instance and
   * hence encapsulated in {@link CassandraDataSource DataSource}
   */
  public abstract @Nullable String keyspaceName();

  public static Builder builder() {
    return new AutoValue_CassandraSchemaReference.Builder();
  }

  /**
   * Returns a stable unique name to be used in PTransforms.
   *
   * @return name of the {@link CassandraSchemaReference}
   */
  public String getName() {
    return "KeySpace." + keyspaceName();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setKeyspaceName(@Nullable String value);

    public abstract CassandraSchemaReference build();
  }
}
