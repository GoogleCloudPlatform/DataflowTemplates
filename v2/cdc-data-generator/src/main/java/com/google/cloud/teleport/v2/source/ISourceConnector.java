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

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.io.IOException;
import java.util.List;

/** Interface for source-specific CDC data generator connectors. */
public interface ISourceConnector {

  /**
   * Returns the source type name (e.g. "SPANNER", "MYSQL").
   *
   * @return source type string
   */
  String getSourceType();

  /**
   * Returns the corresponding {@link SinkType} enum.
   *
   * @return sink type
   */
  SinkType getSinkType();

  /**
   * Parses the sink configuration file from the given path.
   *
   * @param sinkOptionsPath path to the configuration file (e.g. GCS or local file)
   * @return parsed sink configuration
   * @throws IOException if configuration cannot be read
   */
  SinkConfig parseConfig(String sinkOptionsPath) throws IOException;

  /**
   * Returns a new schema fetcher instance for this source.
   *
   * @return schema fetcher
   */
  SinkSchemaFetcher getSchemaFetcher();

  /**
   * Returns the type mapper for this source.
   *
   * @return type mapper
   */
  TypeMapper getTypeMapper();

  /**
   * Creates a new {@link DataWriter} instance for this source with the given config.
   *
   * @param sinkConfig the parsed sink configuration
   * @return data writer instance
   */
  DataWriter createDataWriter(SinkConfig sinkConfig);

  /**
   * Resolves the default key parallelism for this source and sink configuration.
   *
   * @param sinkConfig the parsed sink configuration
   * @return key parallelism count
   */
  int resolveKeyParallelism(SinkConfig sinkConfig);

  /**
   * Returns the logical shard IDs for this source and sink configuration.
   *
   * @param sinkConfig the parsed sink configuration
   * @return list of logical shard IDs, or empty list if not applicable
   */
  List<String> getLogicalShardIds(SinkConfig sinkConfig);
}
