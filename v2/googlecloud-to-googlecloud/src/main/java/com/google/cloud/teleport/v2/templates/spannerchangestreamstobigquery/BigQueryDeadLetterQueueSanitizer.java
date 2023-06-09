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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;

/**
 * Class {@link BigQueryDeadLetterQueueSanitizer} cleans and prepares failed BigQuery inserts to be
 * stored in a GCS Dead Letter Queue. NOTE: The input to a Sanitizer is flexible but the output must
 * be a String unless your override formatMessage().
 */
public final class BigQueryDeadLetterQueueSanitizer
    extends DeadLetterQueueSanitizer<BigQueryInsertError, String> {

  public BigQueryDeadLetterQueueSanitizer() {}

  @Override
  public String getJsonMessage(BigQueryInsertError input) {
    TableRow tableRow = input.getRow();
    /** Extract the original payload from the {@link TableRow}. */
    String message =
        (String) tableRow.get(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON);
    return message;
  }

  @Override
  public String getErrorMessageJson(BigQueryInsertError input) {
    return input.getError().toString();
  }
}
