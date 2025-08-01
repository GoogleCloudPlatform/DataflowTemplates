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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.google.auto.value.AutoOneOf;
import java.io.Serializable;

/**
 * Encapsulates details of a Cassandra Cluster. Cassandra Cluster can connect to multiple KeySpaces,
 * just like a Mysql instance can have multiple databases.
 */
@AutoOneOf(CassandraDataSource.CassandraDialect.class)
public abstract class CassandraDataSource implements Serializable {
  public enum CassandraDialect {
    OSS,
    ASTRA
  };

  public abstract CassandraDataSourceOss oss();

  public abstract AstraDbDataSource astra();

  public static CassandraDataSource ofOss(CassandraDataSourceOss oss) {
    return AutoOneOf_CassandraDataSource.oss(oss);
  }

  public static CassandraDataSource ofAstra(AstraDbDataSource astra) {
    return AutoOneOf_CassandraDataSource.astra(astra);
  }

  public abstract CassandraDialect getDialect();

  public String loggedKeySpace() {
    return switch (getDialect()) {
      case OSS -> oss().loggedKeySpace();
      case ASTRA -> astra().keySpace();
    };
  }
}
