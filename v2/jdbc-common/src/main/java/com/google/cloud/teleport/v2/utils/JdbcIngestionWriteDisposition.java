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

import java.util.EnumMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

/**
 * Exposes WriteDispositionOptions, WriteDispositionException and MapWriteDisposition for
 * DataplexJdbcIngestion template.
 */
public class JdbcIngestionWriteDisposition {

  /**
   * Provides the possible WriteDispositionOptions when writing to BigQuery/GCS and target exists.
   */
  public enum WriteDispositionOptions {
    WRITE_APPEND("WRITE_APPEND"),
    WRITE_TRUNCATE("WRITE_TRUNCATE"),
    WRITE_EMPTY("WRITE_EMPTY"),
    SKIP("SKIP");

    private final String writeDispositionOption;

    WriteDispositionOptions(String writeDispositionOption) {
      this.writeDispositionOption = writeDispositionOption;
    }
  }

  /**
   * Thrown if {@link WriteDispositionOptions} is set to {@code WRITE_EMPTY} and a target file
   * exists.
   */
  public static class WriteDispositionException extends RuntimeException {

    public WriteDispositionException(String message) {
      super(message);
    }
  }

  /** Maps WriteDispostionOptions to BigQueryIO.Write.WriteDisposition. */
  public static class MapWriteDisposition {

    private static final Map<WriteDispositionOptions, WriteDisposition> WRITE_DISPOSITION_MAP;

    static {
      WRITE_DISPOSITION_MAP = new EnumMap<>(WriteDispositionOptions.class);
      WRITE_DISPOSITION_MAP.put(
          WriteDispositionOptions.WRITE_APPEND, WriteDisposition.WRITE_APPEND);
      WRITE_DISPOSITION_MAP.put(
          WriteDispositionOptions.WRITE_TRUNCATE, WriteDisposition.WRITE_TRUNCATE);
      WRITE_DISPOSITION_MAP.put(WriteDispositionOptions.WRITE_EMPTY, WriteDisposition.WRITE_EMPTY);
    }

    public static WriteDisposition mapWriteDispostion(WriteDispositionOptions option) {
      if (option == WriteDispositionOptions.SKIP) {
        throw new UnsupportedOperationException(
            "SKIP WriteDisposition not implemented for writing to BigQuery.");
      }
      return WRITE_DISPOSITION_MAP.get(option);
    }
  }
}
