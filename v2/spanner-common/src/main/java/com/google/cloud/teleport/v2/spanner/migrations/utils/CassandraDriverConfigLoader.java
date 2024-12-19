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
import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDriverConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraDriverConfigLoader.class);

  public static DriverConfigLoader load(String path) throws FileNotFoundException {
    URL url = loadSingleFile(path);
    LOG.debug("Loaded Cassandra Driver config from path {}", path);
    return DriverConfigLoader.fromUrl(url);
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

  private CassandraDriverConfigLoader() {}
}
