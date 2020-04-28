/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.connector;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * <h3>Configuration</h3>
 *
 * <p>The connector expects configuration to be passed via Properties files. By default, the
 * connector will look for a properties file in {@literal /etc/dataflow_cdc.properties}, and it
 * expects the following parameters:</p>
 *
 * * {@literal instanceName} - the instance name on GCP.
 * * {@literal databaseUsername} - a user with privileges to access the binary log for MySQL.
 * * {@literal databasePassword} - the password to use to log into the database. This parameter can
 *     be passed with the default properties file, or in a separate properties file
 *     (default: {@literal /etc/dataflow_cdc_password.properties}).
 * * {@literal databaseAddress} - the IP or DNS address for a MySQL database.
 * * {@literal databasePort} - the port to connect to the database. Default is 3306.
 * * {@literal gcpProject} - the GCP project where the PubSub topic with updates resides.
 * * {@literal gcpPubsubTopicPrefix} - the prefix to PubSub topics to push updates for each MySQL table.
 * * {@literal whitelistedTables} - a comma-separated list of tables to post updates. The name of
 *     the table should be fully qualified
 *     (e.g. "myinstance.mydb.mytable1,myinstance.mydb2.mytable2").
 * * {@literal inMemoryOffsetStorage} - true/false whether or not to store changelog offsets in
 *     memory. Setting this to true means that the connector is not resilient to restarts. This
 *     configuration is generally useful for ephemeral tests (default: {@literal false}).
 * * {@literal offsetStorageFile} the file to use to store changelog offsets from MySQL. This is
 *     necessary on restarts of the connector
 *     (default: {@literal /opt/dataflow-cdc/offset-tracker}).
 *
 * <p>To override the default properties files, addresses can be passed to them. For example, to
 * override the default properties file: </p>
 *
 * {@code java -jar App.jar /users/home/myuser/config/my_dataflow_cdc.properties}
 *
 * <p>To override the default properties file, as well as the default password file:</p>
 *
 * {@code java -jar App.jar /users/home/myuser/config/my_dataflow_cdc.properties /users/home/myuser/config/my_password.properties}
 *
 * <p>The connector also expects <b>Google Cloud credential configuration</b> to be passed via:</p>
 *
 * * The {@literal GOOGLE_APPLICATION_CREDENTIALS} environment set to point to a JSON credential
 *     with access to PubSub, and Data Catalog.
 *
 */
public class App {

    private static final Object MISSING = new Object();

    public static final String DEFAULT_PROPERTIES_FILE_LOCATION = "/etc/dataflow-cdc/dataflow_cdc.properties";

    public static final String PASSWORD_FILE_LOCATION = "/etc/dataflow-cdc/dataflow_cdc_password.properties";

    public static final String DEFAULT_OFFSET_STORAGE_FILE = "/opt/dataflow-cdc/offset/offset-tracker";

    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(App.class);

        // Printing the information about the bindings for SLF4J:
        Configuration config = getConnectorConfiguration(args);
        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
        System.out.println("Logger Binding: " + binder.getLoggerFactory());
        System.out.println(binder.getLoggerFactoryClassStr());

        String dbPassword = config.getString("databasePassword");
        config.clearProperty("databasePassword");
        logger.info("Configuration for program (with DB password hidden) is: \n{}",
            ConfigurationUtils.toString(config));
        config.setProperty("databasePassword", dbPassword);

        logger.info("GOOGLE_APPLICATION_CREDENTIALS: {}",
            System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

        startSender(
            config.getString("databaseName"),
            config.getString("databaseUsername"),
            config.getString("databasePassword"),
            config.getString("databaseAddress"),
            config.getString("databasePort", "3306"),  // MySQL default port is 3306
            config.getString("gcpProject"),
            config.getString("gcpPubsubTopicPrefix"),
            config.getString("offsetStorageFile", DEFAULT_OFFSET_STORAGE_FILE),
            config.getBoolean("inMemoryOffsetStorage", false),
            config.getString("whitelistedTables"));
    }

    /**
     * Load the application configuration to start the MySQL CDC connector.
     *
     * This method has the following scenarios:
     *
     * * Get the main properties file:
     *   * If zero arguments are passed, it uses the default properties file location.
     *   * If more than zero arguments are passed, it uses the first argument to get a properties
     *       file.
     * * If necessary, get the password file:
     *   * If the first properties file does not contain the "databasePassword" property, then it
     *       tries to read the second properties file, which only contains the database password.
     *   * If less than 2 arguments are passed, then use the default password file location.
     *   * If 2 arguments are passed, it uses the second argument to get a password file.
     *   * Add the "databasePassword" property from this file to the main configuration.
     *
     * @param args are the arguments passed to the application via the console.
     * @return
     * @throws ConfigurationException
     */
    static Configuration getConnectorConfiguration(String[] args)
        throws ConfigurationException {
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
        Boolean inMemoryOffsetStorage,
        String commaSeparatedWhiteListedTables) {
        DebeziumMysqlToPubSubDataSender dataSender = new DebeziumMysqlToPubSubDataSender(
            databaseName,
            databaseUserName,
            databasePassword,
            databaseAddress,
            Integer.parseInt(databasePort),
            gcpProject,
            gcpPubsubTopic,
            offsetStorageFile,
            inMemoryOffsetStorage,
            new HashSet<>(Arrays.asList(commaSeparatedWhiteListedTables.split(","))));
        dataSender.run();
    }
}
