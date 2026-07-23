/*
 * Copyright (C) 2026 Google LLC
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

import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomDataGeneratorFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(CustomDataGeneratorFetcher.class);
  private static final ConcurrentHashMap<String, CustomDataGenerator> cache =
      new ConcurrentHashMap<>();

  public static CustomDataGenerator getCustomDataGenerator(
      String customJarPath, String customClassName) {
    if (customJarPath == null
        || customJarPath.isEmpty()
        || customClassName == null
        || customClassName.isEmpty()) {
      return null;
    }

    String cacheKey = customJarPath + ":" + customClassName;
    return cache.computeIfAbsent(
        cacheKey,
        key -> {
          try {
            URL[] urls = JarFileReader.saveFilesLocally(customJarPath);
            URLClassLoader classLoader = URLClassLoader.newInstance(urls);
            Class<?> clazz = classLoader.loadClass(customClassName);
            return (CustomDataGenerator) clazz.getDeclaredConstructor().newInstance();
          } catch (Exception e) {
            throw new RuntimeException(
                "Failed to load CustomDataGenerator from "
                    + customJarPath
                    + " class: "
                    + customClassName,
                e);
          }
        });
  }
}
