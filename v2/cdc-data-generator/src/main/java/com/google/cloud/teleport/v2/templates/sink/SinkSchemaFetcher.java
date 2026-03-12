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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import java.io.IOException;

public interface SinkSchemaFetcher {

  /**
   * Initializes the fetcher with the given JSON configuration.
   *
   * @param jsonData The JSON string containing configuration options specific to the sink.
   */
  void init(String sinkConfigPath);

  /**
   * Fetches the schema definition from the sink.
   *
   * @return The DataGeneratorSchema object.
   * @throws IOException If an error occurs during fetching.
   */
  DataGeneratorSchema getSchema() throws IOException;

  /**
   * Sets the QPS (Queries Per Second) for the data generation.
   *
   * @param insertQps The target QPS.
   */
  void setInsertQps(int insertQps);
}
