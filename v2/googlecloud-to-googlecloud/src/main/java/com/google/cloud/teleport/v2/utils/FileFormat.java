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

/** Exposes FileFormatOptions. */
public class FileFormat {

  /** Possible output file formats supported by Dataplex Templates. */
  public enum FileFormatOptions {
    PARQUET(".parquet"),
    AVRO(".avro"),
    ORC(".orc");

    private final String fileSuffix;

    FileFormatOptions(String fileSuffix) {
      this.fileSuffix = fileSuffix;
    }

    public String getFileSuffix() {
      return fileSuffix;
    }
  }
}
