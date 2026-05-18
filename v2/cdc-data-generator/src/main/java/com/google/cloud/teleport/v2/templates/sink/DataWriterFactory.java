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
import com.google.cloud.teleport.v2.templates.mysql.MySqlDataWriter;
import com.google.cloud.teleport.v2.templates.spanner.SpannerDataWriter;

/**
 * Factory class for creating {@link DataWriter} instances based on the configured {@link SinkType}.
 */
public class DataWriterFactory {

  private DataWriterFactory() {}

  /**
   * Creates a {@link DataWriter} for the specified sink type and configuration path.
   *
   * @param type the sink type to create a writer for
   * @param configPath the path to the sink configuration document
   * @return a new {@link DataWriter} instance
   * @throws IllegalArgumentException if the sink type is unsupported
   */
  public static DataWriter createWriter(SinkType type, String configPath) {
    switch (type) {
      case MYSQL:
        return new MySqlDataWriter(configPath);
      case SPANNER:
        return new SpannerDataWriter(configPath);
      default:
        throw new IllegalArgumentException("Unsupported sink type: " + type);
    }
  }
}
