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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 *
 *
 * <h3>Configuration</h3>
 *
 * <p>The connector expects configuration to be passed via Properties files. By default, the
 * connector will look for a properties file in {@literal /etc/dataflow_cdc.properties}, and it
 * expects the following parameters:
 *
 * <ul>
 *   <li>{@literal databaseName} - the name of the database instance.
 *   <li>{@literal databaseUsername} - a user with privileges to access the binary log for MySQL.
 *   <li>{@literal databasePassword} - the password to use to log into the database. This parameter
 *       can be passed with the default properties file, or in a separate properties file (default:
 *       {@literal /etc/dataflow_cdc_password.properties}).
 *   <li>{@literal databaseAddress} - the IP or DNS address for a MySQL database.
 *   <li>{@literal databasePort} - the port to connect to the database. Default is 3306.
 *   <li>{@literal gcpProject} - the GCP project where the PubSub topic with updates resides.
 *   <li>{@literal gcpPubsubTopicPrefix} - the prefix to PubSub topics to push updates for each
 *       MySQL table.
 *   <li>{@literal whitelistedTables} - a comma-separated list of tables to monitor. The name of the
 *       table should be fully qualified (e.g.
 *       "myinstance.mydb.mytable1,myinstance.mydb2.mytable2").
 *   <li>{@literal inMemoryOffsetStorage} - true/false whether or not to store changelog offsets in
 *       memory. Setting this to true means that the connector is not resilient to restarts. This
 *       configuration is generally useful for ephemeral tests (default: {@literal false}).
 *   <li>{@literal offsetStorageFile} the file to use to store changelog offsets from MySQL. This is
 *       necessary on restarts of the connector (default: {@literal
 *       /opt/dataflow-cdc/offset-tracker}).
 *   <li>{@literal databaseManagementSystem} the kind of database that the connector will connect
 *       to. Options are: {@literal mysql}, {@literal postgres}. (default: {@literal mysql}).
 *   <li>{@literal singleTopicMode} - true/false whether to publish changes from all tables into a
 *       single PubSub topic, or into a separate topic for every database table to use. If this
 *       option is set to {@literal true}, then updates will be pushed to the PubSub topic provided
 *       in {@literal gcpPubsubTopicPrefix}. (default: {@literal false}).
 * </ul>
 *
 * <p>To override the default properties files, addresses can be passed to them. For example, to
 * override the default properties file: {@code java -jar App.jar
 * /users/home/myuser/config/my_dataflow_cdc.properties}
 *
 * <p>To override the default properties file, as well as the default password file: {@code java
 * -jar App.jar /users/home/myuser/config/my_dataflow_cdc.properties
 * /users/home/myuser/config/my_password.properties}
 *
 * <p>The connector also expects <b>Google Cloud credential configuration</b> to be passed via: The
 * {@literal GOOGLE_APPLICATION_CREDENTIALS} environment set to point to a JSON credential with
 * access to PubSub, and Data Catalog.
 */
public class App {

  private static final Object MISSING = new Object();

  public static final String DEFAULT_PROPERTIES_FILE_LOCATION =
      "/etc/dataflow-cdc/dataflow_cdc.properties";

  public static final String PASSWORD_FILE_LOCATION =
      "/etc/dataflow-cdc/dataflow_cdc_password.properties";

  public static final String DEFAULT_OFFSET_STORAGE_FILE =
      "/opt/dataflow-cdc/offset/offset-tracker";
  public static final String DEFAULT_DATABASE_HISTORY_FILE =
      "/opt/dataflow-cdc/offset/database-history.dat";

  public static final String DEFAULT_RDBMS = "mysql";

  public static void main(String[] args) throws Exception {
    final Logger logger = LoggerFactory.getLogger(App.class);

    // Printing the information about the bindings for SLF4J:
    Configuration config = getConnectorConfiguration(args);
    final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
    System.out.println("Logger Binding: " + binder.getLoggerFactory());
    System.out.println(binder.getLoggerFactoryClassStr());

    String dbPassword = config.getString("databasePassword");
    config.clearProperty("databasePassword");
    logger.info(
        "Configuration for program (with DB password hidden) is: \n{}",
        ConfigurationUtils.toString(config));
    config.setProperty("databasePassword", dbPassword);

    logger.info(
        "GOOGLE_APPLICATION_CREDENTIALS: {}", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

    // Properties to be passed directly to Debezium
    ImmutableConfiguration debeziumConfig = config.immutableSubset("debezium");

    startSender(
        checkParsing(config.getString("databaseName"), "databaseName", logger),
        checkParsing(config.getString("databaseUsername"), "databaseUsername", logger),
        checkParsing(config.getString("databasePassword"), "databasePassword", logger),
        checkParsing(config.getString("databaseAddress"), "databaseAddress", logger),
        checkParsing(
            config.getString("databasePort", "3306"),
            "databasePort",
            logger), // MySQL default port is 3306
        checkParsing(config.getString("gcpProject"), "gcpProject", logger),
        checkParsing(config.getString("gcpPubsubTopicPrefix"), "gcpPubsubTopicPrefix", logger),
        checkParsing(
            config.getString("offsetStorageFile", DEFAULT_OFFSET_STORAGE_FILE),
            "offsetStorageFile",
            logger),
        checkParsing(
            config.getString("databaseHistoryFile", DEFAULT_DATABASE_HISTORY_FILE),
            "databaseHistoryFile",
            logger),
        config.getBoolean("inMemoryOffsetStorage", false),
        config.getBoolean("singleTopicMode", false),
        checkParsing(config.getString("whitelistedTables"), "whitelistedTables", logger),
        checkParsing(
            config.getString("databaseManagementSystem", DEFAULT_RDBMS),
            "databaseManagementSystem",
            logger),
        debeziumConfig);
  }

  /**
   * Handle messy parsing.
   *
   * <p>This method gives warning when the parsed property is bounded with quotation marks. This
   * helps debugging when potential messy parsing exists.
   *
   * @param parsedString is the String parsed by config
   * @param propertyName is the String name of the property being parsed
   * @param logger is the logger for printing warnings
   * @return parsedString is the String parsed by config
   */
  private static String checkParsing(String parsedString, String propertyName, Logger logger) {
    if (parsedString.startsWith("\"") || parsedString.startsWith("'")) {
      logger.warn("{} starts with a quotation mark. Please make sure it's intended", propertyName);
    }
    return parsedString;
  }

  /**
   * Load the application configuration to start the MySQL CDC connector.
   *
   * <p>This method has the following scenarios:
   *
   * <p>Get the main properties file:
   *
   * <p>If zero arguments are passed, it uses the default properties file location.
   *
   * <p>If more than zero arguments are passed, it uses the first argument to get a properties file.
   *
   * <p>If necessary, get the password file:
   *
   * <p>If the first properties file does not contain the "databasePassword" property, then it tries
   * to read the second properties file, which only contains the database password.
   *
   * <p>If less than 2 arguments are passed, then use the default password file location.
   *
   * <p>If 2 arguments are passed, it uses the second argument to get a password file.
   *
   * <p>Add the "databasePassword" property from this file to the main configuration.
   *
   * @param args the arguments passed to the application via the console.
   * @return Configuration instance.
   * @throws ConfigurationException if there is an error when parsing configuration file.
   */
  static Configuration getConnectorConfiguration(String[] args) throws ConfigurationException {
    Configurations configs = new Configurations();

    String propertiesFile = args.length == 0 ? DEFAULT_PROPERTIES_FILE_LOCATION : args[0];

    Configuration result = configs.properties(new File(propertiesFile));

    if (result.get(Object.class, "databasePassword", MISSING) == MISSING) {
      String passwordFile = args.length < 2 ? PASSWORD_FILE_LOCATION : args[1];

      Configuration passwordConfig = configs.properties(new File(passwordFile));
      result.addProperty("databasePassword", passwordConfig.getString("databasePassword"));
    }

    return result;
  }

  static void startSender(
      String databaseName,
      String databaseUserName,
      String databasePassword,
      String databaseAddress,
      String databasePort,
      String gcpProject,
      String gcpPubsubTopic,
      String offsetStorageFile,
      String databaseHistoryFile,
      Boolean inMemoryOffsetStorage,
      Boolean singleTopicMode,
      String commaSeparatedWhiteListedTables,
      String rdbms,
      ImmutableConfiguration debeziumConfig) {
    checkNotNull(databaseName, "Please provide a databaseName parameter. Got %s", databaseName);
    checkNotNull(
        databaseUserName, "Please provide a databaseUserName parameter. Got %s", databaseUserName);
    checkNotNull(
        databasePassword, "Please provide a databasePassword parameter. Got %s", databasePassword);
    checkNotNull(databasePort, "Please provide a databasePort parameter. Got %s", databasePort);
    checkNotNull(gcpProject, "Please provide a gcpProject parameter. Got %s", gcpProject);
    checkNotNull(
        gcpPubsubTopic, "Please provide a gcpPubsubTopicPrefix parameter. Got %s", gcpPubsubTopic);
    checkNotNull(
        rdbms,
        "Please provide a databaseManagementSystem parameter."
            + " This can be either mysql or postgres. Got %s",
        rdbms);
    DebeziumToPubSubDataSender dataSender =
        new DebeziumToPubSubDataSender(
            databaseName,
            databaseUserName,
            databasePassword,
            databaseAddress,
            Integer.parseInt(databasePort),
            gcpProject,
            gcpPubsubTopic,
            offsetStorageFile,
            databaseHistoryFile,
            inMemoryOffsetStorage,
            singleTopicMode,
            new HashSet<>(Arrays.asList(commaSeparatedWhiteListedTables.split(","))),
            rdbms,
            debeziumConfig);
    dataSender.run();
  }
}
