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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.io.Resources;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CassandraConfigFileReaderTest {
  private CassandraConfigFileReader cassandraConfigFileReader;
  private MockedStatic<JarFileReader> mockFileReader;

  @BeforeEach
  void setUp() {
    cassandraConfigFileReader = new CassandraConfigFileReader();
    mockFileReader = mockStatic(JarFileReader.class);
  }

  @AfterEach
  void tearDown() {
    if (mockFileReader != null) {
      mockFileReader.close();
    }
  }

  @Test
  void testGetCassandraShardSuccess() {
    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("test-cassandra-config.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
        .thenReturn(new URL[] {testUrl});

    List<Shard> shards = cassandraConfigFileReader.getCassandraShard(testGcsPath);

    assertNotNull(shards, "The shards list should not be null.");
    assertEquals(1, shards.size(), "The shards list should contain one shard.");

    Logger logger = LoggerFactory.getLogger(CassandraConfigFileReader.class);
    assertNotNull(logger, "Logger should be initialized.");
  }

  @Test
  void testGetCassandraShardFileNotFound() {
    String testConfigPath = "gs://non-existent-bucket/non-existent-file.yaml";

    try (MockedStatic<CassandraDriverConfigLoader> mockedConfigLoader =
        mockStatic(CassandraDriverConfigLoader.class)) {
      mockedConfigLoader
          .when(() -> CassandraDriverConfigLoader.getOptionsMapFromFile(testConfigPath))
          .thenThrow(new FileNotFoundException("File not found: " + testConfigPath));

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> cassandraConfigFileReader.getCassandraShard(testConfigPath),
              "Expected an IllegalArgumentException for missing configuration file.");

      assertTrue(
          exception.getMessage().contains("Configuration file not found:"),
          "Exception message should indicate the missing configuration file.");
    }
  }
}
