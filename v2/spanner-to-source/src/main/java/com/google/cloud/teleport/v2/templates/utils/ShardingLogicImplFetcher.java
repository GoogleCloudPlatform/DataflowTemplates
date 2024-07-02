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
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
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
        URL[] classLoaderUrls = saveFilesLocally(customJarPath);

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

  private static URL[] saveFilesLocally(String driverJars) {
    List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(driverJars);

    final String destRoot = Files.createTempDir().getAbsolutePath();
    List<URL> driverJarUrls = new ArrayList<>();
    listOfJarPaths.stream()
        .forEach(
            jarPath -> {
              try {
                ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                @SuppressWarnings("nullness")
                File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                ResourceId destResourceId =
                    FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                copy(sourceResourceId, destResourceId);
                LOG.info("Localized jar: " + sourceResourceId + " to: " + destResourceId);
                driverJarUrls.add(destFile.toURI().toURL());
              } catch (IOException e) {
                LOG.warn("Unable to copy " + jarPath, e);
              }
            });
    return driverJarUrls.stream().toArray(URL[]::new);
  }

  private static void copy(ResourceId source, ResourceId dest) throws IOException {
    try (ReadableByteChannel rbc = FileSystems.open(source)) {
      try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
        ByteStreams.copy(rbc, wbc);
      }
    }
  }
}
