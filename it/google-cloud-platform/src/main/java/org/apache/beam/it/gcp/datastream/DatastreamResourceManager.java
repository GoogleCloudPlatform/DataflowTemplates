/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.datastream;

import static org.apache.beam.it.gcp.datastream.DatastreamResourceManagerUtils.generateDatastreamId;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.datastream.v1.AvroFileFormat;
import com.google.cloud.datastream.v1.BigQueryDestinationConfig;
import com.google.cloud.datastream.v1.BigQueryProfile;
import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.ConnectionProfileName;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DatastreamSettings;
import com.google.cloud.datastream.v1.DeleteConnectionProfileRequest;
import com.google.cloud.datastream.v1.DeleteStreamRequest;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.GcsDestinationConfig;
import com.google.cloud.datastream.v1.GcsProfile;
import com.google.cloud.datastream.v1.JsonFileFormat;
import com.google.cloud.datastream.v1.LocationName;
import com.google.cloud.datastream.v1.MysqlProfile;
import com.google.cloud.datastream.v1.MysqlSourceConfig;
import com.google.cloud.datastream.v1.OracleProfile;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.PostgresqlProfile;
import com.google.cloud.datastream.v1.PostgresqlSourceConfig;
import com.google.cloud.datastream.v1.PrivateConnectivity;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.StreamName;
import com.google.cloud.datastream.v1.UpdateStreamRequest;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for Datastream resources.
 *
 * <p>This class is thread safe.
 */
public final class DatastreamResourceManager implements ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResourceManager.class);
  private static final String FIELD_STATE = "state";

  private static final java.time.Duration DEFAULT_BQ_STALENESS_DURATION =
      java.time.Duration.ofMinutes(15);

  private final String testId;
  private final String projectId;
  private final String location;

  private final Set<String> createdStreamIds;
  private final Set<String> createdConnectionProfileIds;

  private final PrivateConnectivity privateConnectivity;

  private final DatastreamClient datastreamClient;

  public enum DestinationOutputFormat {
    AVRO_FILE_FORMAT,
    JSON_FILE_FORMAT
  }

  private DatastreamResourceManager(Builder builder) throws IOException {
    this(
        DatastreamClient.create(
            DatastreamSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()),
        builder);
  }

  @VisibleForTesting
  public DatastreamResourceManager(DatastreamClient datastreamClient, Builder builder) {
    this.datastreamClient = datastreamClient;
    this.testId = builder.testName;
    this.projectId = builder.projectId;
    this.location = builder.location;
    this.createdStreamIds = Collections.synchronizedSet(new HashSet<>());
    this.createdConnectionProfileIds = Collections.synchronizedSet(new HashSet<>());
    this.privateConnectivity = builder.privateConnectivity;
  }

  public static Builder builder(String testName, String projectId, String location) {
    return new Builder(testName, projectId, location);
  }

  /**
   * Helper method to construct and send a Connection Profile.
   *
   * @param connectionProfileBuilder The Connection Profile Builder with instance-specific
   *     parameters set.
   * @param connectionProfileId The display name of the Connection Profile.
   * @return The Connection Profile that was created in Datastream.
   */
  private synchronized ConnectionProfile createConnectionProfile(
      ConnectionProfile.Builder connectionProfileBuilder, String connectionProfileId)
      throws ExecutionException, InterruptedException {
    connectionProfileBuilder.setDisplayName(connectionProfileId);

    if (privateConnectivity != null) {
      connectionProfileBuilder.setPrivateConnectivity(privateConnectivity);
    } else {
      connectionProfileBuilder.setStaticServiceIpConnectivity(
          StaticServiceIpConnectivity.getDefaultInstance());
    }

    CreateConnectionProfileRequest request =
        CreateConnectionProfileRequest.newBuilder()
            .setParent(LocationName.of(projectId, location).toString())
            .setConnectionProfile(connectionProfileBuilder)
            .setConnectionProfileId(connectionProfileId)
            .build();

    ConnectionProfile reference = datastreamClient.createConnectionProfileAsync(request).get();
    createdConnectionProfileIds.add(connectionProfileId);
    return reference;
  }

  /**
   * Creates a Source Connection Profile for a JDBC server. Supported JDBC types are - MySql,
   * Postgres and Oracle.
   *
   * @param connectionProfileId The ID of the connection profile.
   * @param source An object representing the JDBC source.
   * @return A Datastream JDBC source connection profile.
   */
  private synchronized ConnectionProfile createJDBCSourceConnectionProfile(
      String connectionProfileId, JDBCSource source) {

    LOG.info(
        "Creating JDBC Source Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);

    ConnectionProfile.Builder connectionProfileBuilder = ConnectionProfile.newBuilder();

    switch (source.type()) {
      case MYSQL:
        MysqlProfile.Builder mysqlProfileBuilder = MysqlProfile.newBuilder();
        mysqlProfileBuilder
            .setHostname(source.hostname())
            .setUsername(source.username())
            .setPassword(source.password())
            .setPort(source.port());
        connectionProfileBuilder.setMysqlProfile(mysqlProfileBuilder);
        break;
      case ORACLE:
        OracleProfile.Builder oracleProfileBuilder = OracleProfile.newBuilder();
        oracleProfileBuilder
            .setHostname(source.hostname())
            .setUsername(source.username())
            .setPassword(source.password())
            .setPort(source.port())
            .setDatabaseService(((OracleSource) source).database());
        connectionProfileBuilder.setOracleProfile(oracleProfileBuilder);
        break;
      case POSTGRESQL:
        PostgresqlProfile.Builder postgresqlProfileBuilder = PostgresqlProfile.newBuilder();
        postgresqlProfileBuilder
            .setHostname(source.hostname())
            .setUsername(source.username())
            .setPassword(source.password())
            .setPort(source.port())
            .setDatabase(((PostgresqlSource) source).database());
        connectionProfileBuilder.setPostgresqlProfile(postgresqlProfileBuilder);
        break;
      default:
        throw new DatastreamResourceManagerException(
            "Could not recognize JDBC source type " + source.type().name());
    }

    try {

      ConnectionProfile reference =
          createConnectionProfile(connectionProfileBuilder, connectionProfileId);

      LOG.info(
          "Successfully created JDBC Source Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);

      return reference;

    } catch (Exception e) {
      throw new DatastreamResourceManagerException(
          "Failed to create JDBC source connection profile.", e);
    }
  }

  /**
   * Creates a Source Configuration for a JDBC server. Supported JDBC types are - MySql, Postgres
   * and Oracle.
   *
   * @param sourceConnectionProfileId The ID of the connection profile.
   * @param source An object representing the JDBC source.
   * @return a SourceConfig object which is required to configure a Stream.
   */
  public synchronized SourceConfig buildJDBCSourceConfig(
      String sourceConnectionProfileId, JDBCSource source) {

    ConnectionProfile connectionProfile =
        createJDBCSourceConnectionProfile(
            generateDatastreamId(testId + "-" + sourceConnectionProfileId), source);
    SourceConfig.Builder sourceConfigBuilder =
        SourceConfig.newBuilder().setSourceConnectionProfile(connectionProfile.getName());

    switch (source.type()) {
      case MYSQL:
        sourceConfigBuilder.setMysqlSourceConfig((MysqlSourceConfig) source.config());
        break;
      case POSTGRESQL:
        sourceConfigBuilder.setPostgresqlSourceConfig((PostgresqlSourceConfig) source.config());
        break;
      case ORACLE:
        sourceConfigBuilder.setOracleSourceConfig((OracleSourceConfig) source.config());
        break;
      default:
        throw new DatastreamResourceManagerException(
            "Invalid JDBC source type "
                + source.type().name()
                + ". Must be one of MySQL, Postgres or Oracle.");
    }

    return sourceConfigBuilder.build();
  }

  /**
   * Creates a Connection Profile for a GCS bucket.
   *
   * @param connectionProfileId The ID of the GCS connection profile.
   * @param gcsBucketName The GCS Bucket to connect to.
   * @return A Datastream GCS destination connection profile.
   */
  synchronized ConnectionProfile createGCSDestinationConnectionProfile(
      String connectionProfileId, String gcsBucketName) {

    LOG.info(
        "Creating GCS Destination Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);

    ConnectionProfile.Builder connectionProfileBuilder =
        ConnectionProfile.newBuilder()
            .setGcsProfile(GcsProfile.newBuilder().setBucket(gcsBucketName).setRootPath("/"));

    try {

      ConnectionProfile reference =
          createConnectionProfile(connectionProfileBuilder, connectionProfileId);

      LOG.info(
          "Successfully created GCS Destination Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);

      return reference;

    } catch (Exception e) {
      throw new DatastreamResourceManagerException(
          "Failed to create GCS destination connection profile.", e);
    }
  }

  /**
   * Creates a Destination Configuration for a GCS destination bucket.
   *
   * @param connectionProfileId The ID of the connection profile.
   * @param path The Path prefix to specific GCS location.
   * @param destinationOutputFormat The format of the files written to GCS.
   * @return A DestinationConfig object representing a GCS destination configuration.
   */
  public synchronized DestinationConfig buildGCSDestinationConfig(
      String connectionProfileId,
      String gcsBucketName,
      String path,
      DestinationOutputFormat destinationOutputFormat) {

    ConnectionProfile connectionProfile =
        createGCSDestinationConnectionProfile(
            generateDatastreamId(testId + "-" + connectionProfileId), gcsBucketName);

    DestinationConfig.Builder destinationConfigBuilder =
        DestinationConfig.newBuilder().setDestinationConnectionProfile(connectionProfile.getName());

    GcsDestinationConfig.Builder gcsDestinationConfigBuilder =
        GcsDestinationConfig.newBuilder().setPath(path);

    if (destinationOutputFormat == DestinationOutputFormat.AVRO_FILE_FORMAT) {
      gcsDestinationConfigBuilder.setAvroFileFormat(AvroFileFormat.getDefaultInstance());
    } else {
      gcsDestinationConfigBuilder.setJsonFileFormat(JsonFileFormat.getDefaultInstance());
    }

    destinationConfigBuilder.setGcsDestinationConfig(gcsDestinationConfigBuilder);
    return destinationConfigBuilder.build();
  }

  /**
   * Creates a Connection Profile for a BigQuery instance.
   *
   * @param connectionProfileId The ID of the connection profile.
   * @return A Datastream BigQuery destination connection profile.
   */
  synchronized ConnectionProfile createBQDestinationConnectionProfile(String connectionProfileId) {

    LOG.info(
        "Creating BQ Destination Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);

    ConnectionProfile.Builder connectionProfileBuilder =
        ConnectionProfile.newBuilder().setBigqueryProfile(BigQueryProfile.newBuilder());

    try {

      ConnectionProfile reference =
          createConnectionProfile(connectionProfileBuilder, connectionProfileId);

      LOG.info(
          "Successfully created BQ Destination Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);

      return reference;

    } catch (Exception e) {
      throw new DatastreamResourceManagerException(
          "Failed to create BQ destination connection profile. ", e);
    }
  }

  /**
   * Creates a Destination Configuration for a single BigQuery destination dataset.
   *
   * @param connectionProfileId The ID of the connection profile.
   * @param datasetId The ID of the BigQuery dataset.
   * @return A DestinationConfig object representing a BigQuery destination configuration.
   */
  public synchronized DestinationConfig buildBQDestinationConfig(
      String connectionProfileId, DatasetId datasetId) {
    return buildBQDestinationConfig(connectionProfileId, DEFAULT_BQ_STALENESS_DURATION, datasetId);
  }

  /**
   * Creates a Destination Configuration for a single BigQuery destination dataset.
   *
   * @param connectionProfileId The ID of the connection profile.
   * @param stalenessLimit The desired data freshness.
   * @param datasetId The ID of the BigQuery dataset.
   * @return A DestinationConfig object representing a BigQuery destination configuration.
   */
  public synchronized DestinationConfig buildBQDestinationConfig(
      String connectionProfileId, java.time.Duration stalenessLimit, DatasetId datasetId) {

    ConnectionProfile connectionProfile =
        createBQDestinationConnectionProfile(
            generateDatastreamId(testId + "-" + connectionProfileId));

    DestinationConfig.Builder destinationConfigBuilder =
        DestinationConfig.newBuilder().setDestinationConnectionProfile(connectionProfile.getName());

    BigQueryDestinationConfig.Builder bigQueryDestinationConfig =
        BigQueryDestinationConfig.newBuilder()
            .setDataFreshness(Duration.newBuilder().setSeconds(stalenessLimit.getSeconds()))
            .setSingleTargetDataset(
                BigQueryDestinationConfig.SingleTargetDataset.newBuilder()
                    .setDatasetId(datasetId.getProject() + ":" + datasetId.getDataset()));

    destinationConfigBuilder.setBigqueryDestinationConfig(bigQueryDestinationConfig);
    return destinationConfigBuilder.build();
  }

  /**
   * Creates a Datastream stream from the given source to the given destination.
   *
   * @param streamId The ID of the stream.
   * @param sourceConfig A SourceConfig object representing the source configuration.
   * @param destinationConfig A DestinationConfig object representing the destination configuration.
   * @return A Datastream stream object.
   */
  public synchronized Stream createStream(
      String streamId, SourceConfig sourceConfig, DestinationConfig destinationConfig) {
    return createStream(
        streamId, sourceConfig, destinationConfig, Stream.BackfillAllStrategy.getDefaultInstance());
  }

  /**
   * Creates a Datastream stream from the given source to the given destination.
   *
   * @param streamId The ID of the stream.
   * @param sourceConfig A SourceConfig object representing the source configuration.
   * @param destinationConfig A DestinationConfig object representing the destination configuration.
   * @param backfillAllStrategy
   * @return A Datastream stream object.
   */
  public synchronized Stream createStream(
      String streamId,
      SourceConfig sourceConfig,
      DestinationConfig destinationConfig,
      Stream.BackfillAllStrategy backfillAllStrategy) {

    streamId = generateDatastreamId(testId + "-" + streamId);
    LOG.info("Creating Stream {} in project {}.", streamId, projectId);

    try {
      CreateStreamRequest request =
          CreateStreamRequest.newBuilder()
              .setParent(LocationName.format(projectId, location))
              .setStreamId(streamId)
              .setStream(
                  Stream.newBuilder()
                      .setDisplayName(streamId)
                      .setSourceConfig(sourceConfig)
                      .setDestinationConfig(destinationConfig)
                      .setBackfillAll(backfillAllStrategy)
                      .build())
              .build();

      Stream reference = datastreamClient.createStreamAsync(request).get();
      createdStreamIds.add(streamId);

      LOG.info("Successfully created Stream {} in project {}.", streamId, projectId);
      return reference;
    } catch (Exception e) {
      throw new DatastreamResourceManagerException("Failed to create stream. ", e);
    }
  }

  public synchronized Stream updateStreamState(Stream stream, Stream.State state) {

    LOG.info("Updating {}'s state to {} in project {}.", stream.getName(), state.name(), projectId);

    try {

      FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder().addPaths(FIELD_STATE);

      UpdateStreamRequest request =
          UpdateStreamRequest.newBuilder()
              .setStream(stream.toBuilder().setState(state))
              .setUpdateMask(fieldMaskBuilder)
              .build();

      Stream reference = datastreamClient.updateStreamAsync(request).get();

      LOG.info(
          "Successfully updated {}'s state to {} in project {}.",
          stream.getName(),
          state.name(),
          projectId);

      return reference;
    } catch (Exception e) {
      throw new DatastreamResourceManagerException("Failed to update stream.", e);
    }
  }

  public synchronized Stream startStream(Stream stream) {
    LOG.info("Starting Stream {} in project {}.", stream.getName(), projectId);
    return updateStreamState(stream, Stream.State.RUNNING);
  }

  public synchronized Stream pauseStream(Stream stream) {
    LOG.info("Pausing Stream {} in project {}.", stream.getName(), projectId);
    return updateStreamState(stream, Stream.State.PAUSED);
  }

  public synchronized void cleanupAll() {
    LOG.info("Cleaning up Datastream resource manager.");
    boolean producedError = false;

    try {
      for (String stream : createdStreamIds) {
        datastreamClient
            .deleteStreamAsync(
                DeleteStreamRequest.newBuilder()
                    .setName(StreamName.format(projectId, location, stream))
                    .build())
            .get();
      }
      LOG.info("Successfully deleted stream(s). ");
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to delete stream(s).");
      producedError = true;
    }

    try {
      for (String connectionProfile : createdConnectionProfileIds) {
        datastreamClient
            .deleteConnectionProfileAsync(
                DeleteConnectionProfileRequest.newBuilder()
                    .setName(ConnectionProfileName.format(projectId, location, connectionProfile))
                    .build())
            .get();
      }
      LOG.info("Successfully deleted connection profile(s). ");
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to delete connection profile(s).");
      producedError = true;
    }

    try {
      datastreamClient.close();
    } catch (Exception e) {
      LOG.error("Failed to close datastream client. ");
      producedError = true;
    }

    if (producedError) {
      throw new DatastreamResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    LOG.info("Successfully cleaned up Datastream resource manager.");
  }

  /** Builder for {@link DatastreamResourceManager}. */
  public static final class Builder {
    private final String testName;
    private final String projectId;
    private final String location;

    private CredentialsProvider credentialsProvider;

    private PrivateConnectivity privateConnectivity;

    private Builder(String testName, String projectId, String location) {
      this.testName = testName;
      this.projectId = projectId;
      this.location = location;
    }

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder setPrivateConnectivity(String privateConnectionName) {
      this.privateConnectivity =
          PrivateConnectivity.newBuilder()
              .setPrivateConnection(
                  String.join(
                      "/",
                      "projects",
                      projectId,
                      "locations",
                      location,
                      "privateConnections",
                      privateConnectionName))
              .build();
      return this;
    }

    public DatastreamResourceManager build() throws IOException {
      return new DatastreamResourceManager(this);
    }
  }
}
