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

import com.dtsx.astra.sdk.db.DbOpsClient;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.GuardedStringValueProvider;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.exception.AstraDBNotFoundException;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates details of a Cassandra Cluster. Cassandra Cluster can connect to multiple KeySpaces,
 * just like a Mysql instance can have multiple databases.
 */
@AutoValue
public abstract class AstraDbDataSource implements Serializable {

  abstract String databaseId();

  abstract GuardedStringValueProvider astraToken();

  abstract String keySpace();

  abstract String astraDbRegion();

  private static final Logger LOG = LoggerFactory.getLogger(AstraDbDataSource.class);

  @Memoized
  public byte[] secureConnectBundle() {
    return this.downloadAstraSecureBundle();
  }

  /**
   * Number of Partitions to read from Cassandra.
   *
   * <p>Defaults to Null, which causes CassandraIO to default the number of partitions to number of
   * hosts. TODO(vardhanvthigle): Auto infer Number of partitions based on size estimates table.
   */
  @Nullable
  public abstract Integer numPartitions();

  public static Builder builder() {
    return new AutoValue_AstraDbDataSource.Builder().setNumPartitions(null).setAstraDbRegion("");
  }

  public abstract Builder toBuilder();

  /**
   * Download the Astra DB token (if passed via secret manager) and Security Bundle.
   *
   * @return a pair with the token and the secure bundle
   */
  private byte[] downloadAstraSecureBundle() {

    String astraToken = astraToken().get();
    /*
     * Accessing the devops Api to retrieve the secure bundle.
     */
    DbOpsClient astraDbClient = new DbOpsClient(astraToken, databaseId());
    if (!astraDbClient.exist()) {
      throw new AstraDBNotFoundException(
          "Astra Database does not exist, please check your Astra Token and Database ID. Please ensure that the database is active.");
    }
    byte[] astraSecureBundle;
    if (StringUtils.isEmpty(astraDbRegion())) {
      astraSecureBundle = astraDbClient.downloadDefaultSecureConnectBundle();
    } else {
      astraSecureBundle = astraDbClient.downloadSecureConnectBundle(astraDbRegion());
    }
    LOG.info("Astra Bundle is parsed, length={}", astraSecureBundle.length);
    return astraSecureBundle;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDatabaseId(String value);

    abstract Builder setAstraToken(GuardedStringValueProvider value);

    public Builder setAstraToken(String value) {
      String astraToken = value;
      if (!astraToken.startsWith("AstraCS")) {
        astraToken = SecretManagerUtils.getSecret(value);
      }
      LOG.info("Astra Token is parsed");
      return this.setAstraToken(GuardedStringValueProvider.create(value));
    }

    public abstract Builder setKeySpace(String value);

    public abstract Builder setAstraDbRegion(String value);

    public abstract Builder setNumPartitions(@Nullable Integer value);

    abstract AstraDbDataSource autoBuild();

    public AstraDbDataSource build() {
      return autoBuild();
    }
  }
}
