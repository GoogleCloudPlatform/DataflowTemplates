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
package com.google.cloud.teleport.v2.neo4j.telemetry;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jTelemetry {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jTelemetry.class);

  private static String neo4jDriverVersion = null;
  private static final String DEFAULT_NEO4J_DRIVER_VERSION = "dev";

  public static String userAgent(String templateVersion) {
    return String.format(
        "neo4j-dataflow/%s (%s) %s %s",
        templateVersion, platform(), neo4jDriverVersion(), jreInformation());
  }

  public static Map<String, Object> transactionMetadata(Map<String, Object> metadata) {
    return Map.of("app", "dataflow", "metadata", metadata);
  }

  private static String platform() {
    return String.format(
        "%s; %s; %s",
        System.getProperty("os.name"),
        System.getProperty("os.version"),
        System.getProperty("os.arch"));
  }

  private static String neo4jDriverVersion() {
    if (neo4jDriverVersion == null) {
      neo4jDriverVersion = loadNeo4jDriverVersion();
    }
    return neo4jDriverVersion;
  }

  private static String loadNeo4jDriverVersion() {
    try (InputStream input = Neo4jTelemetry.class.getResourceAsStream("/versions.properties")) {
      var props = new Properties();
      props.load(input);
      return props.getProperty("neo4j-java-driver", DEFAULT_NEO4J_DRIVER_VERSION);
    } catch (Exception e) {
      LOG.warn(
          "Failed to load neo4j-java-driver version. Returning the default value: "
              + DEFAULT_NEO4J_DRIVER_VERSION,
          e);
      return DEFAULT_NEO4J_DRIVER_VERSION;
    }
  }

  private static String jreInformation() {
    // this format loosely follows the Java driver's Bolt Agent format
    return String.format(
        "Java/%s (%s; %s; %s)",
        System.getProperty("java.version"),
        System.getProperty("java.vm.vendor"),
        System.getProperty("java.vm.name"),
        System.getProperty("java.vm.version"));
  }
}
