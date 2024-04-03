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

import java.util.Map;
import org.neo4j.driver.Config;

public class Neo4jTelemetry {

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
    return Config.defaultConfig().userAgent();
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
