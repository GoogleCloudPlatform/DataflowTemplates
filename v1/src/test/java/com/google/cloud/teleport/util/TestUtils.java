/*
 * Copyright (C) 2018 Google LLC
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

import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;

/** The {@link TestUtils} class provides common utilities used for executing the unit tests. */
public class TestUtils {

  /**
   * Helper to generate files for testing.
   *
   * @param filePath The path to the file to write.
   * @param lines The lines to write.
   * @return The file written.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  public static ResourceId writeToFile(String filePath, List<String> lines) throws IOException {

    return writeToFile(filePath, lines, Compression.UNCOMPRESSED);
  }

  /**
   * Helper to generate files for testing.
   *
   * @param filePath The path to the file to write.
   * @param lines The lines to write.
   * @param compression The compression type of the file.
   * @return The file written.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  public static ResourceId writeToFile(String filePath, List<String> lines, Compression compression)
      throws IOException {

    String fileContents = String.join(System.lineSeparator(), lines);

    ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

    String mimeType = compression == Compression.UNCOMPRESSED ? MimeTypes.TEXT : MimeTypes.BINARY;

    // Write the file contents to the channel and close.
    try (ReadableByteChannel readChannel =
        Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
      try (WritableByteChannel writeChannel =
          compression.writeCompressed(FileSystems.create(resourceId, mimeType))) {
        ByteStreams.copy(readChannel, writeChannel);
      }
    }

    return resourceId;
  }

  /**
   * Helper to sort a json object lexicographically.
   *
   * @param jsonObject The json object that is to be sorted
   * @return The sorted json object
   */
  public static JsonObject sortJsonObject(JsonObject jsonObject) {
    TreeMap<String, JsonElement> sortedMap = new TreeMap<>();

    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      String key = entry.getKey();
      JsonElement value = entry.getValue();

      if (value.isJsonObject()) {
        // Recursively sorting multi-level json objects
        sortedMap.put(key, sortJsonObject(value.getAsJsonObject()));
      } else {
        sortedMap.put(key, value);
      }
    }

    JsonObject sortedJsonObject = new JsonObject();
    // Putting the sortedTreeMap values into a json object
    for (Map.Entry<String, JsonElement> entry : sortedMap.entrySet()) {
      sortedJsonObject.add(entry.getKey(), entry.getValue());
    }

    return sortedJsonObject;
  }
}
