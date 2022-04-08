/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.util;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.io.IOUtils;

/**
 * The {@link GCSUtils} class provides common utilities for reading files from Google Cloud Storage.
 */
public class GCSUtils {

  /**
   * The {@link GCSUtils#getGcsFileAsBytes(String)} reads a file from GCS and returns it as raw
   * bytes.
   *
   * @param filePath path to file in GCS
   * @return raw contents of file
   * @throws RuntimeException thrown if not able to read or parse file
   */
  public static byte[] getGcsFileAsBytes(String filePath) throws IOException {
    ReadableByteChannel channel = getGcsFileByteChannel(filePath);
    try (InputStream inputStream = Channels.newInputStream(channel)) {
      return IOUtils.toByteArray(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error when reading: " + filePath, e);
    }
  }

  /** Handles getting the {@link ReadableByteChannel} for {@code filePath}. */
  private static ReadableByteChannel getGcsFileByteChannel(String filePath) throws IOException {
    try {
      MatchResult result = FileSystems.match(filePath);
      checkArgument(
          result.status() == MatchResult.Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filePath);

      List<ResourceId> rId =
          result.metadata().stream().map(MatchResult.Metadata::resourceId).collect(toList());

      checkArgument(rId.size() == 1, "Expected exactly 1 file, but got " + rId.size() + " files.");

      return FileSystems.open(rId.get(0));
    } catch (IOException e) {
      throw new RuntimeException("Error when finding: " + filePath, e);
    }
  }
}
