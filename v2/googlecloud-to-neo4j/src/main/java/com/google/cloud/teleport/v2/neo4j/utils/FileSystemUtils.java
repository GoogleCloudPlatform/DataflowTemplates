/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;

/** Utilities for reading local and remote file resources. */
public class FileSystemUtils {

  public static String getPathContents(String gsPath) throws IOException {

    try (ReadableByteChannel chan =
            FileSystems.open(FileSystems.matchNewResource(gsPath, /* isDirectory= */ false));
        InputStream inputStream = Channels.newInputStream(chan)) {

      return new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
    }
  }
}
