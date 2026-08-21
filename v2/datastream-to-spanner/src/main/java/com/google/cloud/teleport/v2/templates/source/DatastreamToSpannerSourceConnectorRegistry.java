/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.source;

import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.teleport.v2.templates.source.mysql.MySqlDsToSpSourceConnector;
import com.google.cloud.teleport.v2.templates.source.oracle.OracleDsToSpSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgresql.PostgresqlDsToSpSourceConnector;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registry for datastream to spanner source connectors. */
public class DatastreamToSpannerSourceConnectorRegistry {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerSourceConnectorRegistry.class);
  private static final Map<String, IDsToSpSourceConnector> CONNECTORS = new HashMap<>();

  static {
    register(new MySqlDsToSpSourceConnector());
    register(new PostgresqlDsToSpSourceConnector());
    register(new OracleDsToSpSourceConnector());
  }

  private static void register(IDsToSpSourceConnector connector) {
    String type = connector.getSourceType().toLowerCase();
    if (CONNECTORS.containsKey(type)) {
      LOG.warn("Duplicate connector registered for type: {}. Overwriting.", type);
    }
    CONNECTORS.put(type, connector);
    LOG.info("Registered Datastream source connector: {}", type);
  }

  private DatastreamToSpannerSourceConnectorRegistry() {}

  /** Returns the connector for the given source type, or throws if not found. */
  public static IDsToSpSourceConnector getSourceConnector(String sourceType) {
    if (sourceType == null || sourceType.isEmpty()) {
      throw new IllegalArgumentException("Source type cannot be empty");
    }
    IDsToSpSourceConnector connector = CONNECTORS.get(sourceType.toLowerCase());
    if (connector == null) {
      throw new IllegalArgumentException(
          "Unsupported source type: " + sourceType + ". Registered types: " + CONNECTORS.keySet());
    }
    return connector;
  }

  /** Returns all registered connectors. */
  public static Collection<IDsToSpSourceConnector> getConnectors() {
    return Collections.unmodifiableCollection(CONNECTORS.values());
  }

  /** Returns the names of all registered source types. */
  public static Set<String> getSupportedSourceTypes() {
    return Collections.unmodifiableSet(CONNECTORS.keySet());
  }

  /** Identifies the source type from Datastream SourceConfig by querying all connectors. */
  public static String getSourceTypeFromConfig(SourceConfig sourceConfig) {
    for (IDsToSpSourceConnector connector : CONNECTORS.values()) {
      if (connector.matchesSourceConfig(sourceConfig)) {
        return connector.getSourceType();
      }
    }
    throw new IllegalArgumentException("Unsupported source connection profile type in Datastream");
  }
}
