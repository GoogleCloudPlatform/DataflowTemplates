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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Singleton class to get implementation of the shard identification logic. */
public class ShardingLogicImplFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ShardingLogicImplFetcher.class);
  private static IShardIdFetcher shardIdFetcher = null;

  public static synchronized IShardIdFetcher getShardingLogicImpl(
      String customJarPath,
      String shardingCustomClassName,
      String shardingCustomParameters,
      Schema schema,
      String skipDirName) {

    if (shardIdFetcher == null) {
      shardIdFetcher =
          getShardIdFetcherImpl(
              customJarPath,
              shardingCustomClassName,
              shardingCustomParameters,
              schema,
              skipDirName);
    }
    return shardIdFetcher;
  }

  private static IShardIdFetcher getShardIdFetcherImpl(
      String customJarPath,
      String shardingCustomClassName,
      String shardingCustomParameters,
      Schema schema,
      String skipDirName) {
    if (!customJarPath.isEmpty() && !shardingCustomClassName.isEmpty()) {
      LOG.info(
          "Getting custom sharding fetcher : "
              + customJarPath
              + " with class: "
              + shardingCustomClassName);
      try {
        // Get the start time of loading the custom class
        Instant startTime = Instant.now();

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = JarFileReader.saveFilesLocally(customJarPath);

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> shardFetcherClass = urlClassLoader.loadClass(shardingCustomClassName);

        // Create a new instance from the loaded class
        Constructor<?> constructor = shardFetcherClass.getConstructor();
        IShardIdFetcher shardFetcher = (IShardIdFetcher) constructor.newInstance();
        // Get the end time of loading the custom class
        Instant endTime = Instant.now();
        LOG.info(
            "Custom jar "
                + customJarPath
                + ": Took "
                + (new Duration(startTime, endTime)).toString()
                + " to load");
        LOG.info("Invoking init of the custom class with input as {}", shardingCustomParameters);
        shardFetcher.init(shardingCustomParameters);
        return shardFetcher;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    // else return the core implementation
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl(schema, skipDirName);
    return shardIdFetcher;
  }
}
