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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for reading and parsing a Cassandra configuration file from GCS into a
 * CassandraShard object.
 */
public class CassandraConfigFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraConfigFileReader.class);

  /**
   * Reads the Cassandra configuration file from the specified GCS path and converts it into a list
   * of CassandraShard objects.
   *
   * @param cassandraConfigFilePath the GCS path of the Cassandra configuration file.
   * @return a list containing the parsed CassandraShard.
   */
  public List<Shard> getCassandraShard(String cassandraConfigFilePath) {
    try {
      LOG.info("Reading Cassandra configuration from: {}", cassandraConfigFilePath);
      OptionsMap optionsMap =
          CassandraDriverConfigLoader.getOptionsMapFromFile(cassandraConfigFilePath);
      CassandraShard shard = new CassandraShard(optionsMap);
      LOG.info("Successfully created CassandraShard: {}", shard);
      return Collections.singletonList(shard);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(
          "Configuration file not found: " + cassandraConfigFilePath, e);
    }
  }
}
