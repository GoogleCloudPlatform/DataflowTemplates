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

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTACT_POINTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RETRY_POLICY_CLASS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mockStatic;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.typesafe.config.ConfigException;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraDriverConfigLoader}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraDriverConfigLoaderTest {
  MockedStatic mockFileReader;

  @Before
  public void initialize() {
    mockFileReader = mockStatic(JarFileReader.class);
  }

  @Test
  public void testCassandraDriverConfigLoaderBasic()
      throws FileNotFoundException, MalformedURLException {
    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("test-cassandra-config.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
        .thenReturn(new URL[] {testUrl});
    DriverConfigLoader driverConfigLoader = CassandraDriverConfigLoader.loadFile(testGcsPath);
    assertThat(
            driverConfigLoader
                .getInitialConfig()
                .getProfiles()
                .get("default")
                .getStringList(CONTACT_POINTS))
        .isEqualTo(List.of("127.0.0.1:9042", "127.0.0.2:9042"));
    ;
    assertThat(
            driverConfigLoader
                .getInitialConfig()
                .getProfiles()
                .get("default")
                .getString(RETRY_POLICY_CLASS))
        .isEqualTo("DefaultRetryPolicy");
  }

  @Test
  public void testCassandraDriverConfigLoadError()
      throws FileNotFoundException, MalformedURLException {
    String testGcsPathNotFound = "gs://smt-test-bucket/cassandraConfigNotFound.conf";
    String testGcsPathList =
        "gs://smt-test-bucket/cassandraConfig1.conf,gs://smt-test-bucket/cassandraConfig2.conf";

    URL testUrl = Resources.getResource("test-cassandra-config-parse-err.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPathNotFound))
        .thenReturn(new URL[] {});
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPathList))
        .thenReturn(
            new URL[] {
              Resources.getResource("test-cassandra-config.conf"),
              Resources.getResource("test-cassandra-config.conf")
            });
    assertThrows(
        FileNotFoundException.class,
        () -> CassandraDriverConfigLoader.loadFile(testGcsPathNotFound));
    assertThrows(
        IllegalArgumentException.class,
        () -> CassandraDriverConfigLoader.loadFile(testGcsPathList));
  }

  @Test
  public void testCassandraDriverConfigParseError()
      throws FileNotFoundException, MalformedURLException {
    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("test-cassandra-config-parse-err.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
        .thenReturn(new URL[] {testUrl});
    assertThrows(
        ConfigException.Parse.class, () -> CassandraDriverConfigLoader.loadFile(testGcsPath));
  }

  @Test
  public void testOptionsMapConversion() throws FileNotFoundException {

    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("test-cassandra-config.conf");
    mockFileReader
        .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
        .thenReturn(new URL[] {testUrl});
    DriverConfigLoader driverConfigLoaderDirect = CassandraDriverConfigLoader.loadFile(testGcsPath);
    OptionsMap optionsMap = CassandraDriverConfigLoader.getOptionsMapFromFile(testGcsPath);
    DriverConfigLoader driverConfigLoaderFromOptionsMap =
        CassandraDriverConfigLoader.fromOptionsMap(optionsMap);
    ImmutableMap<String, ImmutableMap<String, String>> directLoadMap =
        driverConfigMap(driverConfigLoaderDirect);
    ImmutableMap<String, ImmutableMap<String, String>> fromOptionsMap =
        driverConfigMap(driverConfigLoaderFromOptionsMap);

    assertThat(directLoadMap).isEqualTo(fromOptionsMap);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          OptionsMap optionsMapToLoad = new OptionsMap();
          CassandraDriverConfigLoader.putInOptionsMap(
              optionsMapToLoad, "default", new SimpleEntry<>("Unsupported", "Unsupported"));
        });
  }

  private static ImmutableMap<String, ImmutableMap<String, String>> driverConfigMap(
      DriverConfigLoader driverConfigLoaderDirect) {
    ImmutableMap.Builder<String, ImmutableMap<String, String>> driverConfigMap =
        ImmutableMap.builder();
    driverConfigLoaderDirect
        .getInitialConfig()
        .getProfiles()
        .forEach(
            (profile, options) -> {
              ImmutableMap.Builder<String, String> profileMapBuilder = ImmutableMap.builder();
              options
                  .entrySet()
                  .forEach(
                      e -> profileMapBuilder.put(e.getKey().toString(), e.getValue().toString()));
              driverConfigMap.put(profile, profileMapBuilder.build());
            });
    return driverConfigMap.build();
  }

  @After
  public void cleanup() {
    mockFileReader.close();
    mockFileReader = null;
  }
}
