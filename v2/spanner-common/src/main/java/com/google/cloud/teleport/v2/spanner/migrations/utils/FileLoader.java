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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoader {
  private static final Logger LOG = LoggerFactory.getLogger(FileLoader.class);

  /**
   * Reads the content of a configuration file from a specified file path (e.g., GCS or local).
   *
   * @param sourceConfigFilePath the path or URI to the configuration file
   * @return the content of the file as a string
   * @throws Exception if an error occurs while reading the file
   */
  @VisibleForTesting
  public static String readConfigFilePath(String sourceConfigFilePath) throws Exception {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(sourceConfigFilePath, false)))) {

      return IOUtils.toString(stream, StandardCharsets.UTF_8);
    } catch (IOException e) {
      String errorMessage =
          "Failed to read configuration input file at "
              + sourceConfigFilePath
              + ". Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed HOCON/JSON string.";
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}
