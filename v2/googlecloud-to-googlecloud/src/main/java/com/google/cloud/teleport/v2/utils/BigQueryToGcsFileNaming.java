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
package com.google.cloud.teleport.v2.utils;

import javax.annotation.Nullable;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/** A FileNaming that generates UUID file names for a given directory and file suffix. */
public class BigQueryToGcsFileNaming implements FileNaming {

  private final String tableName;
  private final String partitionName;
  private final String suffix;

  public BigQueryToGcsFileNaming(String suffix, String tableName, @Nullable String partitionName) {
    this.suffix = suffix;
    this.tableName = tableName;
    this.partitionName = partitionName;
  }

  public BigQueryToGcsFileNaming(String suffix, String tableName) {
    this(suffix, tableName, null);
  }

  @Override
  public String getFilename(
      BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
    String filename =
        partitionName != null
            ? String.format("output-%s-%s%s", tableName, partitionName, suffix)
            : String.format("output-%s%s", tableName, suffix);
    return filename;
  }
}
