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

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import com.google.cloud.teleport.v2.templates.mysql.MySqlDataWriter;
import com.google.cloud.teleport.v2.templates.spanner.SpannerDataWriter;

/**
 * Factory class for creating {@link DataWriter} instances based on {@link SinkType} and {@link
 * SinkConfig}. Provides cleaner separation of writer construction logic from the DoFn lifecycle.
 */
public class DataWriterFactory {

  private DataWriterFactory() {}

  /**
   * Creates a {@link DataWriter} instance for the specified sink type and configuration.
   *
   * @param type the target sink type
   * @param config the parsed sink configuration
   * @return a new {@link DataWriter} instance
   * @throws IllegalArgumentException if the sink type is unsupported
   */
  public static DataWriter createWriter(SinkType type, SinkConfig config) {
    switch (type) {
      case MYSQL:
        return new MySqlDataWriter((MySqlSinkConfig) config);
      case SPANNER:
        return new SpannerDataWriter((SpannerSinkConfig) config);
      default:
        throw new IllegalArgumentException("Unsupported sink type: " + type);
    }
  }
}
