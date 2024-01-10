/*
 * Copyright (C) 2019 Google LLC
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

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertErrorCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/** Various utility methods for working with BigQuery. */
public final class BigQueryIOUtils {
  private static final TableReference EMPTY_TABLE_REFERENCE =
      new TableReference().setTableId("unknown").setProjectId("unknown").setDatasetId("unknown");

  private BigQueryIOUtils() {}

  public static void validateBQStorageApiOptionsBatch(BigQueryOptions options) {
    validateStorageWriteApiAtLeastOnce(options);
  }

  public static void validateBQStorageApiOptionsStreaming(BigQueryOptions options) {
    if (options.getUseStorageWriteApi()
        && !options.getUseStorageWriteApiAtLeastOnce()
        && (options.getNumStorageWriteApiStreams() < 1
            || options.getStorageWriteApiTriggeringFrequencySec() == null)) {
      // the number of streams and triggering frequency are only required for exactly-once semantics
      throw new IllegalArgumentException(
          "When streaming with STORAGE_WRITE_API, the number of streams"
              + " (numStorageWriteApiStreams) and triggering frequency"
              + " (storageWriteApiTriggeringFrequencySec) must also be specified.");
    }
    StreamingModeUtils.validateBQOptions(options.as(DataflowPipelineOptions.class));
    validateStorageWriteApiAtLeastOnce(options);
  }

  /** Converts a {@link BigQueryStorageApiInsertError} into a {@link BigQueryInsertError}. */
  public static PCollection<BigQueryInsertError> writeResultToBigQueryInsertErrors(
      WriteResult writeResult, BigQueryOptions options) {
    if (options.getUseStorageWriteApi()) {
      return writeResult
          .getFailedStorageApiInserts()
          .apply(
              MapElements.via(
                  new SimpleFunction<BigQueryStorageApiInsertError, BigQueryInsertError>() {
                    public BigQueryInsertError apply(BigQueryStorageApiInsertError error) {
                      return new BigQueryInsertError(
                          error.getRow(),
                          new InsertErrors()
                              .setErrors(
                                  List.of(new ErrorProto().setMessage(error.getErrorMessage()))),
                          EMPTY_TABLE_REFERENCE);
                    }
                  }))
          .setCoder(BigQueryInsertErrorCoder.of());
    }
    return writeResult.getFailedInsertsWithErr();
  }

  private static void validateStorageWriteApiAtLeastOnce(BigQueryOptions options) {
    if (options.getUseStorageWriteApiAtLeastOnce() && !options.getUseStorageWriteApi()) {
      // Technically this is a no-op, since useStorageWriteApiAtLeastOnce is only checked by
      // BigQueryIO when useStorageWriteApi is true, but it might be confusing to a user why
      // useStorageWriteApiAtLeastOnce doesn't take effect.
      throw new IllegalArgumentException(
          "When at-least-once semantics (useStorageWriteApiAtLeastOnce) are enabled Storage Write"
              + " API (useStorageWriteApi) must also be enabled.");
    }
  }
}
