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
package com.google.cloud.teleport.v2.utils;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/** A FileNaming that generates file names using Write.defaultNaming. */
public class DataplexJdbcIngestionNaming implements FileNaming {

  private final FileNaming defaultNaming;

  public DataplexJdbcIngestionNaming(String prefix, String suffix) {
    defaultNaming = Write.defaultNaming(prefix + "output", suffix);
  }

  public DataplexJdbcIngestionNaming(String suffix) {
    this("", suffix);
  }

  public String getSingleFilename() {
    return getFilename(GlobalWindow.INSTANCE, PaneInfo.NO_FIRING, 1, 0, Compression.AUTO);
  }

  @Override
  public String getFilename(
      BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
    return defaultNaming.getFilename(window, pane, numShards, shardIndex, compression);
  }
}
