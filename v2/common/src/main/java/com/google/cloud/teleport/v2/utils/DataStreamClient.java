/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.datastream.v1.Datastream;
import com.google.api.services.datastream.v1.model.DiscoverConnectionProfileRequest;
import com.google.api.services.datastream.v1.model.MysqlColumn;
import com.google.api.services.datastream.v1.model.MysqlDatabase;
import com.google.api.services.datastream.v1.model.MysqlRdbms;
import com.google.api.services.datastream.v1.model.MysqlTable;
import com.google.api.services.datastream.v1.model.OracleColumn;
import com.google.api.services.datastream.v1.model.OracleRdbms;
import com.google.api.services.datastream.v1.model.OracleSchema;
import com.google.api.services.datastream.v1.model.OracleTable;
import com.google.api.services.datastream.v1.model.PostgresqlColumn;
import com.google.api.services.datastream.v1.model.PostgresqlRdbms;
import com.google.api.services.datastream.v1.model.PostgresqlSchema;
import com.google.api.services.datastream.v1.model.PostgresqlTable;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.api.services.datastream.v1.model.Stream;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataStreamClient} provides access to the DataStream APIs required to process CDC
 * DataStream data and maintain schema aligment.
 */
public class DataStreamClient implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamClient.class);
  private static final Pattern PARENT_IN_CONNECTION_PROFILE_PATTERN =
      Pattern.compile("(projects/.*/locations/.*)/connectionProfiles/.*");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("TIMESTAMP\\(?\\d?\\)?");
  private static final Pattern TIMESTAMP_WITH_TIMEZONE_PATTERN =
      Pattern.compile("TIMESTAMP\\(?\\d?\\)? WITH TIME ZONE");
  private static final Pattern TIMESTAMP_WITH_LOCAL_TIMEZONE_PATTERN =
      Pattern.compile("TIMESTAMP\\(?\\d?\\)? WITH LOCAL TIME ZONE");

  private final Credentials credentials;
  private transient Datastream datastream;
  private String rootUrl = "https://datastream.googleapis.com/";

  private final Counter datastreamRpcs = Metrics.counter(DataStreamClient.class, "datastreamRpcs");

  public DataStreamClient(Credentials credential) throws IOException {
    this.credentials = credential;
  }

  public void setRootUrl(String url) {
    this.rootUrl = url;
  }

  private Datastream getDataStream() throws IOException {
    if (this.datastream == null) {
      try {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        HttpRequestInitializer initializer = getHttpRequestInitializer(credentials);
        this.datastream =
            new Datastream.Builder(httpTransport, jsonFactory, initializer)
                .setApplicationName("BeamDataStreamClient")
                .setRootUrl(this.rootUrl)
                .build();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }
    return this.datastream;
  }

  private static HttpRequestInitializer getHttpRequestInitializer(Credentials credential)
      throws IOException {
    if (credential == null) {
      try {
        return GoogleCredential.getApplicationDefault();
      } catch (Exception e) {
        return new NullCredentialInitializer();
      }
    } else {
      return new HttpCredentialsAdapter(credential);
    }
  }

  public Map<String, StandardSQLTypeName> getObjectSchema(
      String streamName, String schemaName, String tableName) throws IOException {
    SourceConfig sourceConnProfile = getSourceConnectionProfile(streamName);

    if (sourceConnProfile.getMysqlSourceConfig() != null) {
      return getMysqlObjectSchema(streamName, schemaName, tableName, sourceConnProfile);
    } else if (sourceConnProfile.getOracleSourceConfig() != null) {
      return getOracleObjectSchema(streamName, schemaName, tableName, sourceConnProfile);
    } else if (sourceConnProfile.getPostgresqlSourceConfig() != null) {
      return getPostgresqlObjectSchema(streamName, schemaName, tableName, sourceConnProfile);
    } else {
      LOG.error("Source Connection Profile Type Not Supported");
      throw new IOException("Source Connection Profile Type Not Supported");
    }
  }

  public List<String> getPrimaryKeys(String streamName, String schemaName, String tableName)
      throws IOException {
    try {
      SourceConfig sourceConnProfile = getSourceConnectionProfile(streamName);
      if (sourceConnProfile.getMysqlSourceConfig() != null) {
        return getMysqlPrimaryKeys(streamName, schemaName, tableName, sourceConnProfile);
      } else if (sourceConnProfile.getOracleSourceConfig() != null) {
        return getOraclePrimaryKeys(streamName, schemaName, tableName, sourceConnProfile);
      } else if (sourceConnProfile.getPostgresqlSourceConfig() != null) {
        return getPostgresqlPrimaryKeys(streamName, schemaName, tableName, sourceConnProfile);
      } else {
        throw new IOException("Source Connection Profile Type Not Supported");
      }
    } catch (IOException e) {
      if (e.toString()
          .contains(
              "Quota exceeded for quota metric 'API requests' and limit 'API requests per minute'"
                  + " of service")) {
        try {
          Thread.sleep(60 * 1000);
          return this.getPrimaryKeys(streamName, schemaName, tableName);
        } catch (InterruptedException i) {
        }
      }
      throw e;
    }
  }

  private Datastream.Projects.Locations.ConnectionProfiles.Discover getDiscoverTableRequest(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    String sourceConnProfileName = sourceConnProfile.getSourceConnectionProfile();
    String parent = getParentFromConnectionProfileName(sourceConnProfileName);

    DiscoverConnectionProfileRequest discoverRequest =
        new DiscoverConnectionProfileRequest().setConnectionProfileName(sourceConnProfileName);
    if (sourceConnProfile.getMysqlSourceConfig() != null) {
      MysqlRdbms mysqlRdbms = buildMysqlRdbmsForTable(schemaName, tableName);
      discoverRequest = discoverRequest.setMysqlRdbms(mysqlRdbms);
    } else if (sourceConnProfile.getOracleSourceConfig() != null) {
      OracleRdbms oracleRdbms = buildOracleRdbmsForTable(schemaName, tableName);
      discoverRequest = discoverRequest.setOracleRdbms(oracleRdbms);
    } else if (sourceConnProfile.getPostgresqlSourceConfig() != null) {
      PostgresqlRdbms postgresqlRdbms = buildPostgresqlRdbmsForTable(schemaName, tableName);
      discoverRequest = discoverRequest.setPostgresqlRdbms(postgresqlRdbms);
    } else {
      throw new IOException("Source Connection Profile Type Not Supported");
    }

    Datastream.Projects.Locations.ConnectionProfiles.Discover discoverConnProfile =
        getDataStream()
            .projects()
            .locations()
            .connectionProfiles()
            .discover(parent, discoverRequest);
    this.datastreamRpcs.inc();
    return discoverConnProfile;
  }

  /**
   * Return a {@link Stream} with the related information required to request schema discovery.
   *
   * @param streamName The fully qualified Stream name. ie. project/my-project/stream/my-stream
   */
  public Stream getStream(String streamName) throws IOException {

    Datastream.Projects.Locations.Streams.Get getStream =
        getDataStream().projects().locations().streams().get(streamName);
    Stream stream = getStream.execute();
    this.datastreamRpcs.inc();
    return stream;
  }

  /**
   * Return a {@link SourceConfig} ConnectionProfile object which can be used for schema discovery
   * and connection information.
   *
   * @param streamName The ID of a DataStream Stream (ie. project/my-project/stream/my-stream).
   */
  public SourceConfig getSourceConnectionProfile(String streamName) throws IOException {
    Stream stream = getStream(streamName);

    SourceConfig sourceConnProfile = stream.getSourceConfig();

    return sourceConnProfile;
  }

  /**
   * Return a DataStream API parent string representing the base required for a Discovery API
   * request (ie. projects/my-project/locations/my-location).
   *
   * @param connectionProfileName The ID of a ConnectionProfile. ie.
   *     project/my-project/locations/my-location/connectionProfiles/my-connection-profile
   */
  public String getParentFromConnectionProfileName(String connectionProfileName) {
    Matcher m = PARENT_IN_CONNECTION_PROFILE_PATTERN.matcher(connectionProfileName);
    m.find();

    return m.group(1);
  }

  private Map<String, StandardSQLTypeName> getMysqlObjectSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Map<String, StandardSQLTypeName> objectSchema = new HashMap<String, StandardSQLTypeName>();

    MysqlTable table =
        discoverMysqlTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (MysqlColumn column : table.getMysqlColumns()) {
      StandardSQLTypeName bqType = convertMysqlToBigQueryColumnType(column);
      objectSchema.put(column.getColumn(), bqType);
    }
    return objectSchema;
  }

  public List<String> getMysqlPrimaryKeys(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    List<String> primaryKeys = new ArrayList<String>();
    MysqlTable table =
        discoverMysqlTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (MysqlColumn column : table.getMysqlColumns()) {
      Boolean isPrimaryKey = column.getPrimaryKey();
      if (BooleanUtils.isTrue(isPrimaryKey)) {
        primaryKeys.add(column.getColumn());
      }
    }

    return primaryKeys;
  }

  /**
   * Return a {@link MysqlTable} object with schema and PK information.
   *
   * @param streamName A fully qualified Stream name (ie. projects/my-project/stream/my-stream)
   * @param schemaName The name of the schema for the table being discovered.
   * @param tableName The name of the table to discover.
   * @param sourceConnProfile The SourceConfig connection profile to be discovered.
   */
  public MysqlTable discoverMysqlTableSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Datastream.Projects.Locations.ConnectionProfiles.Discover discoverConnProfile =
        getDiscoverTableRequest(streamName, schemaName, tableName, sourceConnProfile);

    MysqlRdbms tableResponse = discoverConnProfile.execute().getMysqlRdbms();
    MysqlDatabase schema = tableResponse.getMysqlDatabases().get(0);
    MysqlTable table = schema.getMysqlTables().get(0);

    return table;
  }

  private MysqlRdbms buildMysqlRdbmsForTable(String databaseName, String tableName) {
    List<MysqlTable> mysqlTables = new ArrayList<MysqlTable>();
    mysqlTables.add(new MysqlTable().setTable(tableName));

    List<MysqlDatabase> mysqlDatabases = new ArrayList<MysqlDatabase>();
    mysqlDatabases.add(new MysqlDatabase().setDatabase(databaseName).setMysqlTables(mysqlTables));

    MysqlRdbms rdbms = new MysqlRdbms().setMysqlDatabases(mysqlDatabases);

    return rdbms;
  }

  public List<String> getOraclePrimaryKeys(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    List<String> primaryKeys = new ArrayList<String>();
    OracleTable table =
        discoverOracleTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (OracleColumn column : table.getOracleColumns()) {
      Boolean isPrimaryKey = column.getPrimaryKey();
      if (BooleanUtils.isTrue(isPrimaryKey)) {
        primaryKeys.add(column.getColumn());
      }
    }

    return primaryKeys;
  }

  private Map<String, StandardSQLTypeName> getOracleObjectSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Map<String, StandardSQLTypeName> objectSchema = new HashMap<String, StandardSQLTypeName>();

    OracleTable table =
        discoverOracleTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (OracleColumn column : table.getOracleColumns()) {
      StandardSQLTypeName bqType = convertOracleToBigQueryColumnType(column);
      objectSchema.put(column.getColumn(), bqType);
    }
    return objectSchema;
  }

  public List<String> getPostgresqlPrimaryKeys(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    List<String> primaryKeys = new ArrayList<String>();
    PostgresqlTable table =
        discoverPostgresqlTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (PostgresqlColumn column : table.getPostgresqlColumns()) {
      Boolean isPrimaryKey = column.getPrimaryKey();
      if (BooleanUtils.isTrue(isPrimaryKey)) {
        primaryKeys.add(column.getColumn());
      }
    }

    return primaryKeys;
  }

  private Map<String, StandardSQLTypeName> getPostgresqlObjectSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Map<String, StandardSQLTypeName> objectSchema = new HashMap<String, StandardSQLTypeName>();

    PostgresqlTable table =
        discoverPostgresqlTableSchema(streamName, schemaName, tableName, sourceConnProfile);
    for (PostgresqlColumn column : table.getPostgresqlColumns()) {
      StandardSQLTypeName bqType = convertPostgresqlToBigQueryColumnType(column);
      objectSchema.put(column.getColumn(), bqType);
    }
    return objectSchema;
  }

  /**
   * Return a {@link OracleTable} object with schema and PK information.
   *
   * @param streamName A fully qualified Stream name (ie. projects/my-project/stream/my-stream)
   * @param schemaName The name of the schema for the table being discovered.
   * @param tableName The name of the table to discover.
   * @param sourceConnProfile The SourceConfig connection profile to be discovered.
   */
  public OracleTable discoverOracleTableSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Datastream.Projects.Locations.ConnectionProfiles.Discover discoverConnProfile =
        getDiscoverTableRequest(streamName, schemaName, tableName, sourceConnProfile);

    OracleRdbms tableResponse = discoverConnProfile.execute().getOracleRdbms();
    OracleSchema schema = tableResponse.getOracleSchemas().get(0);
    OracleTable table = schema.getOracleTables().get(0);

    return table;
  }

  private OracleRdbms buildOracleRdbmsForTable(String schemaName, String tableName) {
    List<OracleTable> oracleTables = new ArrayList<OracleTable>();
    oracleTables.add(new OracleTable().setTable(tableName));

    List<OracleSchema> oracleSchemas = new ArrayList<OracleSchema>();
    oracleSchemas.add(new OracleSchema().setSchema(schemaName).setOracleTables(oracleTables));

    OracleRdbms rdbms = new OracleRdbms().setOracleSchemas(oracleSchemas);

    return rdbms;
  }

  /**
   * Return a {@link PostgresqlTable} object with schema and PK information.
   *
   * @param streamName A fully qualified Stream name (ie. projects/my-project/stream/my-stream)
   * @param schemaName The name of the schema for the table being discovered.
   * @param tableName The name of the table to discover.
   * @param sourceConnProfile The SourceConfig connection profile to be discovered.
   */
  public PostgresqlTable discoverPostgresqlTableSchema(
      String streamName, String schemaName, String tableName, SourceConfig sourceConnProfile)
      throws IOException {
    Datastream.Projects.Locations.ConnectionProfiles.Discover discoverConnProfile =
        getDiscoverTableRequest(streamName, schemaName, tableName, sourceConnProfile);

    PostgresqlRdbms tableResponse = discoverConnProfile.execute().getPostgresqlRdbms();
    PostgresqlSchema schema = tableResponse.getPostgresqlSchemas().get(0);
    PostgresqlTable table = schema.getPostgresqlTables().get(0);

    return table;
  }

  private PostgresqlRdbms buildPostgresqlRdbmsForTable(String schemaName, String tableName) {
    List<PostgresqlTable> postgresqlTables = new ArrayList<PostgresqlTable>();
    postgresqlTables.add(new PostgresqlTable().setTable(tableName));

    List<PostgresqlSchema> postgresqlSchemas = new ArrayList<PostgresqlSchema>();
    postgresqlSchemas.add(
        new PostgresqlSchema().setSchema(schemaName).setPostgresqlTables(postgresqlTables));

    PostgresqlRdbms rdbms = new PostgresqlRdbms().setPostgresqlSchemas(postgresqlSchemas);

    return rdbms;
  }

  public StandardSQLTypeName convertOracleToBigQueryColumnType(OracleColumn column) {
    String dataType = column.getDataType();

    switch (dataType) {
      case "ANYDATA":
      case "BFILE":
      case "CHAR":
      case "CLOB":
      case "NCHAR":
      case "NCLOB":
      case "NVARCHAR2":
      case "ROWID":
      case "UDT":
      case "UROWID":
      case "VARCHAR":
      case "VARCHAR2":
      case "XMLTYPE":
        return StandardSQLTypeName.STRING;
      case "SMALLINT":
      case "INTEGER":
        return StandardSQLTypeName.INT64;
      case "BINARY DOUBLE":
      case "BINARY FLOAT":
      case "BINARY_DOUBLE":
      case "BINARY_FLOAT":
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
        return StandardSQLTypeName.FLOAT64;
      case "DECIMAL":
      case "DOUBLE PRECISION":
      case "NUMBER":
        return StandardSQLTypeName.BIGNUMERIC;
      case "BLOB":
      case "RAW":
      case "LONG_RAW":
        return StandardSQLTypeName.BYTES;
      case "DATE":
        return StandardSQLTypeName.DATETIME;
      default:
    }

    if (TIMESTAMP_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP;
    } else if (TIMESTAMP_WITH_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else if (TIMESTAMP_WITH_LOCAL_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else {
      LOG.warn("Datastream Oracle Type Unknown, Default to String: \"{}\"", dataType);
      return StandardSQLTypeName.STRING;
    }
  }

  public StandardSQLTypeName convertMysqlToBigQueryColumnType(MysqlColumn column) {
    String dataType = column.getDataType().toUpperCase();

    switch (dataType) {
      case "BLOB":
      case "VARCHAR":
      case "CHAR":
      case "TINYTEXT":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
        return StandardSQLTypeName.STRING;
      case "TINYINT":
      case "SMALLINT":
      case "MEDIUMINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
        return StandardSQLTypeName.INT64;
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "DOUBLE PRECISION":
        return StandardSQLTypeName.FLOAT64;
      case "DECIMAL":
      case "NUMERIC":
        return StandardSQLTypeName.BIGNUMERIC;
      case "BINARY":
      case "VARBINARY":
        return StandardSQLTypeName.BYTES;
      case "DATETIME":
        return StandardSQLTypeName.TIMESTAMP;
      case "DATE":
        return StandardSQLTypeName.DATE;
        // (naveronen) - i'm setting this a STRING for now, but some customers might need a
        // different
        // solution. once we encounter such cases, we might need to adjust this
      case "SET":
      case "ENUM":
        return StandardSQLTypeName.STRING;
      case "BIT":
        return StandardSQLTypeName.INT64;
      default:
    }

    if (TIMESTAMP_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP;
    } else if (TIMESTAMP_WITH_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else if (TIMESTAMP_WITH_LOCAL_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else {
      LOG.warn("Datastream MySQL Type Unknown, Default to String: \"{}\"", dataType);
      return StandardSQLTypeName.STRING;
    }
  }

  public StandardSQLTypeName convertPostgresqlToBigQueryColumnType(PostgresqlColumn column) {
    String dataType = column.getDataType().toUpperCase();

    switch (dataType) {
      case "UUID":
      case "CHAR":
      case "CHARACTER":
      case "VARCHAR":
      case "TEXT":
      case "TINYTEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
        return StandardSQLTypeName.STRING;
      case "BOOLEAN":
        return StandardSQLTypeName.BOOL;
      case "TINYINT":
      case "SMALLINT":
      case "MEDIUMINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
        return StandardSQLTypeName.INT64;
      case "DECIMAL":
      case "NUMERIC":
        return StandardSQLTypeName.NUMERIC;
      case "DOUBLE PRECISION":
      case "FLOAT":
      case "REAL":
        return StandardSQLTypeName.FLOAT64;
      case "DATE":
        return StandardSQLTypeName.DATE;
      case "BYTEA":
        return StandardSQLTypeName.BYTES;
      case "TIME":
        return StandardSQLTypeName.TIME;
      case "TIMESTAMP WITH TIME ZONE":
      case "TIMESTAMP":
        return StandardSQLTypeName.TIMESTAMP;
      default:
    }

    if (TIMESTAMP_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP;
    } else if (TIMESTAMP_WITH_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else if (TIMESTAMP_WITH_LOCAL_TIMEZONE_PATTERN.matcher(dataType).matches()) {
      return StandardSQLTypeName.TIMESTAMP; // TODO: what type do we want here?
    } else {
      LOG.warn("Datastream PostgreSQL Type Unknown, Default to String: \"{}\"", dataType);
      return StandardSQLTypeName.STRING;
    }
  }
}
