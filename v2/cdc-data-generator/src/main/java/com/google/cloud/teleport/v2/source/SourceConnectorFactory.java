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
package com.google.cloud.teleport.v2.source;

import com.google.cloud.teleport.v2.source.mysql.MySqlSourceConnector;
import com.google.cloud.teleport.v2.source.oracle.OracleSourceConnector;
import com.google.cloud.teleport.v2.source.spanner.SpannerSourceConnector;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import java.util.Locale;

/** Factory to create {@link ISourceConnector} instances based on sink type or options. */
public class SourceConnectorFactory {

  private SourceConnectorFactory() {}

  /**
   * Gets the appropriate {@link ISourceConnector} for the given {@link SinkType}.
   *
   * @param sinkType the sink type
   * @return the source connector
   */
  public static ISourceConnector getSourceConnector(SinkType sinkType) {
    if (sinkType == null) {
      throw new IllegalArgumentException("Unsupported sink type: null");
    }
    switch (sinkType) {
      case SPANNER:
        return new SpannerSourceConnector();
      case MYSQL:
        return new MySqlSourceConnector();
      case ORACLE:
        return new OracleSourceConnector();
      default:
        throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
    }
  }

  public static ISourceConnector getSourceConnector(String sinkType) {
    if (sinkType == null) {
      throw new IllegalArgumentException("Unsupported sink type: null");
    }
    return getSourceConnector(SinkType.valueOf(sinkType.toUpperCase()));
  }

  /**
   * Gets the appropriate {@link ISourceConnector} from the given {@link CdcDataGeneratorOptions}.
   *
   * @param options pipeline options
   * @return the source connector
   */
  public static ISourceConnector getSourceConnector(CdcDataGeneratorOptions options) {
    if (options == null || options.getSinkType() == null) {
      throw new IllegalArgumentException("Options or sink type must not be null");
    }
    return getSourceConnector(options.getSinkType());
  }

  /**
   * Gets the appropriate {@link ISourceConnector} for the given source type string.
   *
   * @param sourceType the source type name (e.g. "SPANNER", "MYSQL", "ORACLE")
   * @return the source connector
   */
  public static ISourceConnector getSourceConnectorBySourceType(String sourceType) {
    if (sourceType == null || sourceType.trim().isEmpty()) {
      throw new IllegalArgumentException("Source type must not be null or empty");
    }
    String upper = sourceType.trim().toUpperCase(Locale.ROOT);
    switch (upper) {
      case "SPANNER":
        return new SpannerSourceConnector();
      case "MYSQL":
        return new MySqlSourceConnector();
      case "ORACLE":
        return new OracleSourceConnector();
      default:
        throw new IllegalArgumentException("Unsupported source type: " + sourceType);
    }
  }
}
