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

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationImplFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationImplFetcher.class);
  private static ISpannerMigrationTransformer spannerMigrationTransformer = null;

  public static synchronized ISpannerMigrationTransformer getCustomTransformationLogicImpl(
      CustomTransformation customTransformation) {

    if (spannerMigrationTransformer == null) {
      spannerMigrationTransformer = getApplyTransformationImpl(customTransformation);
    }
    return spannerMigrationTransformer;
  }

  public static ISpannerMigrationTransformer getApplyTransformationImpl(
      CustomTransformation customTransformation) {
    if (!customTransformation.jarPath().isEmpty() && !customTransformation.classPath().isEmpty()) {
      LOG.info(
          "Getting spanner migration transformer : "
              + customTransformation.jarPath()
              + " with class: "
              + customTransformation.classPath());
      try {
        // Get the start time of loading the custom class
        Instant startTime = Instant.now();

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = JarFileReader.saveFilesLocally(customTransformation.jarPath());

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> customTransformationClass =
            urlClassLoader.loadClass(customTransformation.classPath());

        // Create a new instance from the loaded class
        Constructor<?> constructor = customTransformationClass.getConstructor();
        ISpannerMigrationTransformer datastreamToSpannerTransformation =
            (ISpannerMigrationTransformer) constructor.newInstance();
        // Get the end time of loading the custom class
        Instant endTime = Instant.now();
        LOG.info(
            "Custom jar "
                + customTransformation.jarPath()
                + ": Took "
                + (new Duration(startTime, endTime)).toString()
                + " to load");
        LOG.info(
            "Invoking init of the custom class with input as {}",
            customTransformation.customParameters());
        datastreamToSpannerTransformation.init(customTransformation.customParameters());
        return datastreamToSpannerTransformation;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    return null;
  }
}
