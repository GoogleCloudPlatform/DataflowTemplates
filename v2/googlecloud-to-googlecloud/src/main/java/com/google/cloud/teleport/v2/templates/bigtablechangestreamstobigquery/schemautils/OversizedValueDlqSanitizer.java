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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.OversizedValue;
import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sanitizes {@link OversizedValue} records into compact JSON lines suitable for the severe dead
 * letter queue. Produces one JSON object per input; keys reuse the canonical BigQuery column names
 * from {@link ChangelogColumn} where applicable.
 */
public final class OversizedValueDlqSanitizer
    extends DeadLetterQueueSanitizer<OversizedValue, String> {

  /** Non-column JSON keys. Keep aligned with the integration test assertions. */
  static final String RAW_BYTES = "raw_bytes";

  static final String ESTIMATED_DECODED_BYTES = "estimated_decoded_bytes";
  static final String MAX_BYTES = "max_bytes";
  static final String REASON = "reason";
  static final String REASON_VALUE = "decoded_value_exceeds_max_bytes";

  private static final ThreadLocal<Gson> GSON = ThreadLocal.withInitial(Gson::new);

  public OversizedValueDlqSanitizer() {}

  @Override
  public String getJsonMessage(OversizedValue input) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put(ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), input.rowKey());
    record.put(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName(), formatCommitTimestamp(input));
    record.put(ChangelogColumn.COLUMN_FAMILY.getBqColumnName(), input.columnFamily());
    record.put(ChangelogColumn.COLUMN.getBqColumnName(), input.column());
    record.put(RAW_BYTES, input.rawBytes());
    record.put(ESTIMATED_DECODED_BYTES, input.estimatedDecodedBytes());
    record.put(MAX_BYTES, input.maxBytes());
    record.put(REASON, REASON_VALUE);
    record.put(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName(), input.sourceInstance());
    record.put(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName(), input.sourceCluster());
    record.put(ChangelogColumn.SOURCE_TABLE.getBqColumnName(), input.sourceTable());
    return GSON.get().toJson(record);
  }

  @Override
  public String getErrorMessageJson(OversizedValue input) {
    return REASON_VALUE;
  }

  private static String formatCommitTimestamp(OversizedValue input) {
    org.threeten.bp.Instant ts = input.commitTimestamp();
    if (ts == null) {
      return null;
    }
    return java.time.Instant.ofEpochSecond(ts.getEpochSecond(), ts.getNano()).toString();
  }
}
