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

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigException;
import java.io.FileNotFoundException;
import java.net.URL;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common static utility class that allows the spanner migration pipelines to ingest Cassandra
 * Driver config file from GCS. Cassandra has a structured config file to accept all the driver
 * parameters, be it list of host ip addresses, credentials, retry policy and many more. Most of
 * these parameters are very specific to the Cassandra Database. Refer to the <a
 * href=>https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/configuration/reference/index.html>reference
 * configuration</a> for the file format.
 */
public final class CassandraDriverConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraDriverConfigLoader.class);
  private static final ImmutableMap<String, TypedDriverOption> OPTIONS_SUPPORTED_BY_DRIVER =
      getOptionsSupportedByDriver();

  /**
   * Load the Cassandra Config from a file as a {@link DriverConfigLoader}.
   *
   * @param path A complete gcs path to the config file of the form "gs://path/to/file".
   * @return DriverConfigLoader.
   * @throws FileNotFoundException - If file is not found at specified path.
   */
  public static DriverConfigLoader loadFile(String path) throws FileNotFoundException {
    URL url = loadSingleFile(path);
    LOG.debug("Loaded Cassandra Driver config from path {}", path);
    try {
      DriverConfigLoader.fromUrl(url).getInitialConfig();
      return DriverConfigLoader.fromUrl(url);
    } catch (ConfigException.Parse parseException) {
      LOG.error(
          "Parsing error while parsing Cassandra Driver config from path {}", path, parseException);
      throw parseException;
    }
  }

  /**
   * Load the Cassandra Config from a file as a {@link java.io.Serializable} {@link OptionsMap}.
   * This {@link OptionsMap} can be stored in any object that needs to implement {@link
   * java.io.Serializable}. At the time of opening a connection to Cassandra, it can be deserialized
   * by {@link CassandraDriverConfigLoader#fromOptionsMap(OptionsMap)}. Note: Implementation Detail,
   * Cassandra Driver does not provide a direct method to convert a link {@link DriverConfigLoader}
   * into an {@link OptionsMap}, or build an {@link OptionsMap} from a file.
   *
   * @param path A complete gcs path to the config file of the form "gs://path/to/file".
   * @return DriverConfigLoader.
   * @throws FileNotFoundException - If file is not found at specified path.
   */
  public static OptionsMap getOptionsMapFromFile(String path) throws FileNotFoundException {
    OptionsMap optionsMap = new OptionsMap();
    DriverConfigLoader configLoader = loadFile(path);
    configLoader
        .getInitialConfig()
        .getProfiles()
        .forEach(
            (profileName, profile) ->
                profile
                    .entrySet()
                    .forEach(
                        e ->
                            putInOptionsMap(
                                optionsMap, profileName, e.getKey(), e.getValue(), profile)));

    return optionsMap;
  }

  /**
   * Load the {@link DriverConfigLoader} from {@link java.io.Serializable} {@link OptionsMap} which
   * was obtained as a part of {@link CassandraDriverConfigLoader#getOptionsMapFromFile(String)}.
   *
   * @param optionsMap
   * @return DriverConfigLoader.
   */
  public static DriverConfigLoader fromOptionsMap(OptionsMap optionsMap) {
    return DriverConfigLoader.fromMap(optionsMap);
  }

  @VisibleForTesting
  protected static URL loadSingleFile(String path) throws FileNotFoundException {
    URL[] urls = JarFileReader.saveFilesLocally(path);
    if (urls.length == 0) {
      LOG.error("Could not load any Cassandra driver config file from specified path {}", path);
      throw (new FileNotFoundException("No file found in path " + path));
    }
    if (urls.length > 1) {
      LOG.error(
          "Need to provide a single Cassandra driver config file in the specified path {}. Found {} ",
          path,
          urls);
      throw (new IllegalArgumentException(
          String.format(
              "Need to provide a single Cassandra driver config file in the specified path %s. Found %d files",
              path, urls.length)));
    }
    return urls[0];
  }

  @VisibleForTesting
  protected static void putInOptionsMap(
      OptionsMap optionsMap,
      String profileName,
      String optionName,
      Object untypedValue,
      DriverExecutionProfile profile) {

    TypedDriverOption option = OPTIONS_SUPPORTED_BY_DRIVER.get(optionName);
    if (Objects.equal(option, null)) {
      LOG.error(
          "Unknown Cassandra Option {}, Options supported by driver = {}",
          optionName,
          OPTIONS_SUPPORTED_BY_DRIVER);
      throw new IllegalArgumentException(
          String.format(
              "Unknown Cassandra Driver Option %s. Supported Options = %s",
              optionName, OPTIONS_SUPPORTED_BY_DRIVER));
    }
    putOptionInOptionsMap(optionsMap, profileName, profile, untypedValue, option);
  }

  @VisibleForTesting
  protected static void putOptionInOptionsMap(
      OptionsMap optionsMap,
      String profileName,
      DriverExecutionProfile profile,
      Object untypedValue,
      TypedDriverOption option) {

    ProfileExtractor profileExtractor =
        TYPED_EXTRACTORS.getOrDefault(option.getExpectedType(), (p, o) -> untypedValue);
    // For "protocol.max-frame-length" are defined as GenericType<Long> in TypedOptions.
    // but the driver Config API needs getBytes for handling size units like MB.
    if (option.equals(TypedDriverOption.PROTOCOL_MAX_FRAME_LENGTH)) {
      profileExtractor = (p, o) -> p.getBytes(o);
    }

    optionsMap.put(profileName, option, profileExtractor.get(profile, option.getRawOption()));
  }

  private static ImmutableMap<String, TypedDriverOption> getOptionsSupportedByDriver() {
    ImmutableMap.Builder<String, TypedDriverOption> mapBuilder = ImmutableMap.builder();
    TypedDriverOption.builtInValues().forEach(e -> mapBuilder.put(e.getRawOption().getPath(), e));
    return mapBuilder.build();
  }

  private interface ProfileExtractor<ValueT> {
    ValueT get(DriverExecutionProfile profile, DriverOption driverOption);
  }

  private static final ImmutableMap<GenericType<?>, ProfileExtractor> TYPED_EXTRACTORS =
      ImmutableMap.<GenericType<?>, ProfileExtractor>builder()
          .put(GenericType.of(Boolean.class), DriverExecutionProfile::getBoolean)
          .put(GenericType.listOf(Boolean.class), DriverExecutionProfile::getBooleanList)
          .put(GenericType.of(Double.class), DriverExecutionProfile::getDouble)
          .put(GenericType.listOf(Double.class), DriverExecutionProfile::getDoubleList)
          .put(GenericType.of(Duration.class), DriverExecutionProfile::getDuration)
          .put(GenericType.listOf(Duration.class), DriverExecutionProfile::getDurationList)
          .put(GenericType.of(Integer.class), DriverExecutionProfile::getInt)
          .put(GenericType.listOf(Integer.class), DriverExecutionProfile::getIntList)
          .put(GenericType.of(Long.class), DriverExecutionProfile::getLong)
          .put(GenericType.listOf(Long.class), DriverExecutionProfile::getLong)
          .put(GenericType.of(String.class), DriverExecutionProfile::getString)
          .put(GenericType.listOf(String.class), DriverExecutionProfile::getStringList)
          .put(GenericType.mapOf(String.class, String.class), DriverExecutionProfile::getStringMap)
          .build();

  private CassandraDriverConfigLoader() {}
}
