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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.io.Resources;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class CassandraConfigFileReaderTest {

  private CassandraConfigFileReader cassandraConfigFileReader;
  private MockedStatic<JarFileReader> mockFileReader;

  @Before
  public void setUp() {
    cassandraConfigFileReader = new CassandraConfigFileReader();
    mockFileReader = Mockito.mockStatic(JarFileReader.class);
  }

  @After
  public void tearDown() {
    if (mockFileReader != null) {
      mockFileReader.close();
    }
  }

  @Test
  public void testGetCassandraShardSuccess() {
    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("test-cassandra-config.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
        .thenReturn(new URL[] {testUrl});

    List<Shard> shards = cassandraConfigFileReader.getCassandraShard(testGcsPath);

    assertNotNull("The shards list should not be null.", shards);
    assertEquals("The shards list should contain one shard.", 1, shards.size());

    Logger logger = LoggerFactory.getLogger(CassandraConfigFileReader.class);
    assertNotNull("Logger should be initialized.", logger);
  }

  @Test
  public void testGetCassandraShardFileNotFound() {
    String testConfigPath = "gs://non-existent-bucket/non-existent-file.yaml";

    try (MockedStatic<CassandraDriverConfigLoader> mockedConfigLoader =
        Mockito.mockStatic(CassandraDriverConfigLoader.class)) {
      mockedConfigLoader
          .when(() -> CassandraDriverConfigLoader.getOptionsMapFromFile(testConfigPath))
          .thenThrow(new FileNotFoundException("File not found: " + testConfigPath));

      IllegalArgumentException exception = null;
      try {
        cassandraConfigFileReader.getCassandraShard(testConfigPath);
      } catch (IllegalArgumentException e) {
        exception = e;
      }

      assertNotNull("Exception should be thrown for missing configuration file.", exception);
      assertTrue(
          "Exception message should indicate the missing configuration file.",
          exception.getMessage().contains("Configuration file not found:"));
    }
  }
}
