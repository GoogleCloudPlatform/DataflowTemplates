/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import com.google.api.client.json.GenericJson;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;

/**
 * Class {@link BigQueryDeadLetterQueueSanitizer} cleans and prepares failed BigQuery inserts to be
 * stored in a GCS Dead Letter Queue. NOTE: The input to a Sanitizer is flexible but the output must
 * be a String unless your override formatMessage().
 */
public final class BigQueryDeadLetterQueueSanitizer
    extends DeadLetterQueueSanitizer<BigQueryStorageApiInsertError, String> {
  private static final ThreadLocal<Gson> gsonThreadLocal = ThreadLocal.withInitial(Gson::new);

  public BigQueryDeadLetterQueueSanitizer() {}

  @Override
  public String getJsonMessage(BigQueryStorageApiInsertError input) {
    // BQ returns final TableRow value it tried to write to BQ, but failed.
    // At this point there is no access to Mod JSON
    GenericJson tableRow = input.getRow();

    Map<String, Object> filtered = new HashMap<>();

    for (ChangelogColumn column : ChangelogColumn.values()) {
      Object value = tableRow.get(column.getBqColumnName());
      if (value != null) {
        filtered.put(column.getBqColumnName(), value);
      }
    }
    return gsonThreadLocal.get().toJson(filtered);
  }

  @Override
  public String getErrorMessageJson(BigQueryStorageApiInsertError input) {
    return input.getErrorMessage();
  }
}
