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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraSourceRowMapperFactoryFn;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Generate Table Reader For Cassandra using the upstream {@link CassandraIO.Read} implementation.
 */
public class CassandraTableReaderFactoryCassandraIoImpl implements CassandraTableReaderFactory {

  /**
   * Returns a Table Reader for given Cassandra Source using the upstream {@link CassandraIO.Read}.
   *
   * <p>Note on Tech debt:
   *
   * <p>Upstream Beam's CassandraIO uses Cassandra Driver's 3.0 driver API. This would mean that the
   * entire breadth of options and configurations possible by 4.0 driver API out of box, notably
   * around SSL connectivity, would not be supported out of box. Currently, we are supporting a very
   * basic use case of client working with SSL enabled server. For a more complete support the
   * strategy is to first commit 4.0 support upstream and then use it in the template.
   *
   * @param cassandraDataSource
   * @param sourceSchemaReference
   * @param sourceTableSchema
   * @return table reader for the source.
   */
  @Override
  public PTransform<PBegin, PCollection<SourceRow>> getTableReader(
      CassandraDataSource cassandraDataSource,
      SourceSchemaReference sourceSchemaReference,
      SourceTableSchema sourceTableSchema) {
    CassandraSourceRowMapper cassandraSourceRowMapper =
        getSourceRowMapper(sourceSchemaReference, sourceTableSchema);
    DriverExecutionProfile profile =
        cassandraDataSource.driverConfigLoader().getInitialConfig().getDefaultProfile();
    final Read<SourceRow> tableReader =
        CassandraIO.<SourceRow>read()
            .withTable(sourceTableSchema.tableName())
            .withHosts(
                cassandraDataSource.contactPoints().stream()
                    .map(p -> p.getHostString())
                    .collect(Collectors.toList()))
            .withPort(cassandraDataSource.contactPoints().get(0).getPort())
            .withKeyspace(cassandraDataSource.loggedKeySpace())
            .withLocalDc(cassandraDataSource.localDataCenter())
            .withConsistencyLevel(
                profile.getString(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY.getRawOption()))
            .withEntity(SourceRow.class)
            .withCoder(SerializableCoder.of(SourceRow.class))
            .withMapperFactoryFn(
                CassandraSourceRowMapperFactoryFn.create(cassandraSourceRowMapper));
    return setSslOptions(setCredentials(tableReader, profile), profile);
  }

  @VisibleForTesting
  protected CassandraIO.Read<SourceRow> setCredentials(
      CassandraIO.Read<SourceRow> tableReader, DriverExecutionProfile profile) {
    if (profile.isDefined(TypedDriverOption.AUTH_PROVIDER_USER_NAME.getRawOption())) {
      tableReader =
          tableReader.withUsername(
              profile.getString(TypedDriverOption.AUTH_PROVIDER_USER_NAME.getRawOption()));
    }
    if (profile.isDefined(TypedDriverOption.AUTH_PROVIDER_PASSWORD.getRawOption())) {
      tableReader =
          tableReader.withPassword(
              profile.getString(TypedDriverOption.AUTH_PROVIDER_PASSWORD.getRawOption()));
    }
    return tableReader;
  }

  @VisibleForTesting
  protected static CassandraIO.Read<SourceRow> setSslOptions(
      CassandraIO.Read<SourceRow> tableReader, DriverExecutionProfile profile) {
    if (enableSSL(profile)) {
      String trustStorePath =
          (profile.isDefined(TypedDriverOption.SSL_TRUSTSTORE_PATH.getRawOption()))
              ? profile.getString(TypedDriverOption.SSL_TRUSTSTORE_PATH.getRawOption())
              : null;
      String trustStorePassword =
          (profile.isDefined(TypedDriverOption.SSL_TRUSTSTORE_PATH.getRawOption()))
              ? profile.getString(TypedDriverOption.SSL_TRUSTSTORE_PASSWORD.getRawOption())
              : null;
      return tableReader.withSsl(
          SSLOptionsProvider.buidler()
              .setSslOptionsFactory(() -> getSSLOptions(trustStorePath, trustStorePassword))
              .build());
    }
    return tableReader;
  }

  @VisibleForTesting
  protected static boolean enableSSL(DriverExecutionProfile profile) {
    return (profile.isDefined(TypedDriverOption.SSL_TRUSTSTORE_PATH.getRawOption())
        || profile.isDefined(TypedDriverOption.SSL_KEYSTORE_PATH.getRawOption()));
  }

  @VisibleForTesting
  protected static SSLOptions getSSLOptions(
      @Nullable String trustStorePath, @Nullable String trustStorePassword) {
    try {
      if (trustStorePath != null) {
        SSLContext sslContext = buildSSLContext(trustStorePath, trustStorePassword);
        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
      } else {
        return RemoteEndpointAwareJdkSSLOptions.builder().build();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static SSLContext buildSSLContext(
      String trustStorePath, @Nullable String trustStorePassword)
      throws java.security.GeneralSecurityException, IOException {
    // Load the default trust store
    TrustManagerFactory defaultTrustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    defaultTrustManagerFactory.init((KeyStore) null);

    // Load your custom trust store
    KeyStore customTrustStore = KeyStore.getInstance("JKS");
    if (trustStorePassword != null) {
      customTrustStore.load(new FileInputStream(trustStorePath), trustStorePassword.toCharArray());
    } else {
      customTrustStore.load(new FileInputStream(trustStorePath), new char[] {});
    }
    TrustManagerFactory customTrustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    customTrustManagerFactory.init(customTrustStore);

    // Create the SSLContext
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        null,
        ArrayUtils.addAll(
            customTrustManagerFactory.getTrustManagers(),
            defaultTrustManagerFactory.getTrustManagers()),
        null);

    return sslContext;
  }

  private CassandraSourceRowMapper getSourceRowMapper(
      SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
    return CassandraSourceRowMapper.builder()
        .setSourceTableSchema(sourceTableSchema)
        .setSourceSchemaReference(sourceSchemaReference)
        .build();
  }
}
