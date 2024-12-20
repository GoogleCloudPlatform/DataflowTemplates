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

import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for reading and parsing a Cassandra configuration file from GCS into a
 * CassandraShard object.
 */
public class CassandraConfigFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraConfigFileReader.class);
  private static final Gson GSON =
      new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.IDENTITY).create();

  /**
   * Reads the Cassandra configuration file from the specified GCS path and converts it into a list
   * of CassandraShard objects.
   *
   * @param cassandraConfigFilePath the GCS path of the Cassandra configuration file.
   * @return a list containing the parsed CassandraShard.
   */
  public List<Shard> getCassandraShard(String cassandraConfigFilePath) {
    try (InputStream stream = getFileInputStream(cassandraConfigFilePath)) {
      String configContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
      CassandraShard shard = GSON.fromJson(configContent, CassandraShard.class);

      LOG.info("Successfully read Cassandra config: {}", shard);
      return Collections.singletonList(shard);
    } catch (IOException e) {
      String errorMessage =
          "Failed to read Cassandra config file. Ensure it is ASCII or UTF-8 encoded and contains a well-formed JSON string.";
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Retrieves an InputStream for the specified GCS file path.
   *
   * @param filePath the GCS file path.
   * @return an InputStream for the file.
   * @throws IOException if the file cannot be accessed or opened.
   */
  private InputStream getFileInputStream(String filePath) throws IOException {
    return Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(filePath, false)));
  }
}
