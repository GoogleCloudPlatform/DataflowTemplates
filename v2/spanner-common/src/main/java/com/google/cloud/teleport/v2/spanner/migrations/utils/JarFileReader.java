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

import java.io.File;
import java.io.IOException;
import java.net.URL;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JarFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(JarFileReader.class);

  public static URL[] saveFilesLocally(String driverJars) {
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
