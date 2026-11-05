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
package com.google.cloud.teleport.v2.templates.sink;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.source.SourceConnectorFactory;
import com.google.cloud.teleport.v2.source.spanner.SpannerSourceConnector;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

/** Utility to parse sink configuration and pre-fetch database dialect on driver JVM. */
public class SinkConfigParser {

  private SinkConfigParser() {}

  @VisibleForTesting
  public interface DialectProvider {
    Dialect getDialect(String projectId, String instanceId, String databaseId);
  }

  @VisibleForTesting
  public static void setDialectProvider(DialectProvider provider) {
    SpannerSourceConnector.setDialectProvider(provider != null ? provider::getDialect : null);
  }

  /**
   * Parses the sink configuration for the specified sink type and options path.
   *
   * @param sinkType the sink type
   * @param sinkOptionsPath path to the sink options configuration file
   * @return parsed sink configuration
   * @throws IOException if configuration cannot be parsed
   */
  public static SinkConfig parse(SinkType sinkType, String sinkOptionsPath) throws IOException {
    if (sinkOptionsPath == null || sinkOptionsPath.isEmpty()) {
      throw new IllegalArgumentException("Sink options path must not be null or empty.");
    }
    return SourceConnectorFactory.getSourceConnector(sinkType).parseConfig(sinkOptionsPath);
  }
}
