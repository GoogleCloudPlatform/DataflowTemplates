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

/** Exposes WriteDispositionOptions and WriteDispositionException. */
public class WriteDisposition {

  /** Provides the possible WriteDispositionOptions when writing to GCS and target file exists. */
  public enum WriteDispositionOptions {
    OVERWRITE("OVERWRITE"),
    SKIP("SKIP"),
    FAIL("FAIL");

    private final String writeDispositionOption;

    WriteDispositionOptions(String writeDispositionOption) {
      this.writeDispositionOption = writeDispositionOption;
    }

    public String getWriteDispositionOption() {
      return writeDispositionOption;
    }
  }

  /**
   * Thrown if {@link com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionOptions
   * WriteDispositionOptions} is set to {@code FAIL} and a target file exists.
   */
  public static class WriteDispositionException extends RuntimeException {
    public WriteDispositionException(String message) {
      super(message);
    }
  }
}
