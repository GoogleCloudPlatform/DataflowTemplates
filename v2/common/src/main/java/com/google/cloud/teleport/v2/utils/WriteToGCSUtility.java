/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * The {@link WriteToGCSUtility} class provides the static values and methods to handle various File
 * Formats.
 */
public class WriteToGCSUtility {

  private WriteToGCSUtility() {}

  /** Set Enum FileFormat for all supported file formats. */
  public enum FileFormat {
    TEXT,
    AVRO,
    PARQUET;
  }

  /** File suffix to be set based on format of the file. */
  public static final ImmutableMap<FileFormat, String> FILE_SUFFIX_MAP =
      Maps.immutableEnumMap(
          ImmutableMap.of(
              FileFormat.TEXT, ".txt",
              FileFormat.AVRO, ".avro",
              FileFormat.PARQUET, ".parquet"));

  /**
   * Shard Template of the output file. Specified as repeating sequences of the letters 'S' or 'N'
   * (example: SSS-NNN).
   */
  public static final String SHARD_TEMPLATE = "W-P-SS-of-NN";

  /**
   * The isValidFileFormat() checks whether user specified file format is valid.
   *
   * @param fileFormat Format of the file given by the user
   * @return status Boolean value indicating valid/invalid status
   */
  public static boolean isValidFileFormat(String fileFormat) {
    boolean status = true;
    try {
      FILE_SUFFIX_MAP.get(FileFormat.valueOf(fileFormat));
    } catch (Exception e) {
      status = false;
    }
    return status;
  }
}
