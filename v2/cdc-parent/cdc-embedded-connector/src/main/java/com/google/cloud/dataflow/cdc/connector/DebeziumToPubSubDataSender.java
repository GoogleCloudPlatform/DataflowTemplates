/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.connector;

import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.relational.history.MemoryDatabaseHistory;
import io.debezium.util.Clock;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the utilities to track Binlog and send data updates to PubSub.
 *
 * <p>Properties of the MySQL connector for Debezium: -
 * https://debezium.io/docs/connectors/mysql/#connector-properties
 */
public class DebeziumToPubSubDataSender implements Runnable {

  public static final String APP_NAME = "debezium-to-pubsub-connector";
  public static final Integer DEFAULT_FLUSH_INTERVAL_MS = 10000;

  public static final ImmutableMap<String, String> RDBMS_TO_CONNECTOR_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("mysql", "io.debezium.connector.mysql.MySqlConnector")
          .put("postgres", "io.debezium.connector.postgresql.PostgresConnector")
          .put("sqlserver", "io.debezium.connector.sqlserver.SqlServerConnector")
          .build();

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumToPubSubDataSender.class);

  private final Configuration config;

  private final String userName;
  private final String userPassword;
  private final String databaseAddress;
  private final Integer databasePort;
  private final String gcpProject;
  private final String gcpPubsubTopicPrefix;
  private final String offsetStorageFile;
  private final String databaseHistoryFile;
  private final Boolean inMemoryOffsetStorage;
  private final Boolean singleTopicMode;

  private final Set<String> whitelistedTables;

  public DebeziumToPubSubDataSender(
      String databaseName,
      String userName,
      String userPassword,
      String databaseAddress,
      Integer databasePort,
      String gcpProject,
      String gcpPubsubTopicPrefix,
      String offsetStorageFile,
      String databaseHistoryFile,
      Boolean inMemoryOffsetStorage,
      Boolean singleTopicMode,
      Set<String> whitelistedTables,
      String rdbms,
      org.apache.commons.configuration2.ImmutableConfiguration debeziumConfig) {

    this.userName = userName;
    this.userPassword = userPassword;
    this.databaseAddress = databaseAddress;
    this.databasePort = databasePort;
    this.gcpProject = gcpProject;
    this.gcpPubsubTopicPrefix = gcpPubsubTopicPrefix;
    this.offsetStorageFile = offsetStorageFile;
    this.databaseHistoryFile = databaseHistoryFile;
    this.inMemoryOffsetStorage = inMemoryOffsetStorage;
    this.singleTopicMode = singleTopicMode;
    this.whitelistedTables = whitelistedTables;

    Preconditions.checkArgument(
        RDBMS_TO_CONNECTOR_MAP.containsKey(rdbms),
        "Unsupported DBMS %s. Only supported DBMS values are %s",
        rdbms,
        String.join(",", RDBMS_TO_CONNECTOR_MAP.keySet()));

    // Prepare Debezium's table.whitelist property by removing
    // instance name from each of the whitelisted tables specified.
    String dbzWhitelistedTables =
        whitelistedTables.stream()
            .map(s -> s.substring(s.indexOf(".") + 1))
            .collect(Collectors.joining(","));

    Configuration.Builder configBuilder =
        Configuration.empty()
            .withSystemProperties(Function.identity())
            .edit()
            .with(EmbeddedEngine.CONNECTOR_CLASS, RDBMS_TO_CONNECTOR_MAP.get(rdbms))
            .with(EmbeddedEngine.ENGINE_NAME, APP_NAME)
            // Database connection information.
            .with("database.hostname", this.databaseAddress)
            .with("database.port", this.databasePort)
            .with("database.user", this.userName)
            .with("database.password", this.userPassword)
            .with("database.server.name", databaseName)
            .with("decimal.handling.mode", "string")
            .with(
                HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
                MemoryDatabaseHistory.class.getName());

    if (!whitelistedTables.isEmpty()) {
      LOG.info("Whitelisting tables: {}", dbzWhitelistedTables);
      configBuilder =
          configBuilder.with(
              RelationalDatabaseConnectorConfig.TABLE_WHITELIST, dbzWhitelistedTables);
    }

    if (this.inMemoryOffsetStorage) {
      LOG.info("Setting up in memory offset storage.");
      configBuilder =
          configBuilder.with(
              EmbeddedEngine.OFFSET_STORAGE,
              "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
    } else {
      LOG.info("Setting up in File-based offset storage in {}.", this.offsetStorageFile);
      configBuilder =
          configBuilder
              .with(
                  EmbeddedEngine.OFFSET_STORAGE,
                  "org.apache.kafka.connect.storage.FileOffsetBackingStore")
              .with(EmbeddedEngine.OFFSET_STORAGE_FILE_FILENAME, this.offsetStorageFile)
              .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS)
              .with(
                  HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
                  FileDatabaseHistory.class.getName())
              .with("database.history.file.filename", this.databaseHistoryFile);
    }

    Iterator<String> keys = debeziumConfig.getKeys();
    while (keys.hasNext()) {
      String configKey = keys.next();
      configBuilder = configBuilder.with(configKey, debeziumConfig.getString(configKey));
    }

    config = configBuilder.build();
  }

  @Override
  public void run() {
    final PubSubChangeConsumer changeConsumer =
        new PubSubChangeConsumer(
            whitelistedTables,
            DataCatalogSchemaUtils.getSchemaManager(
                gcpProject, gcpPubsubTopicPrefix, singleTopicMode),
            PubSubChangeConsumer.DEFAULT_PUBLISHER_FACTORY);

    final EmbeddedEngine engine =
        EmbeddedEngine.create()
            .using(config)
            .using(this.getClass().getClassLoader())
            .using(Clock.SYSTEM)
            .notifying(changeConsumer)
            .build();

    LOG.info("Initializing Debezium Embedded Engine");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future = executor.submit(engine);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Requesting embedded engine to shut down");
                  engine.stop();
                }));

    awaitTermination(executor, future, engine);
  }

  private void awaitTermination(ExecutorService executor, Future<?> future, EmbeddedEngine engine) {

    boolean finalized = false;

    while (!finalized) {
      try {
        future.get(30, TimeUnit.SECONDS);
        if (future.isDone() || future.isCancelled()) {
          engine.stop();
          executor.shutdown();
          break;
        }
      } catch (TimeoutException e) {
        LOG.info("Waiting another 30 seconds for the embedded engine to shut down");
      } catch (CancellationException | InterruptedException | ExecutionException e) {
        finalized = true;
      }
    }
  }
}
