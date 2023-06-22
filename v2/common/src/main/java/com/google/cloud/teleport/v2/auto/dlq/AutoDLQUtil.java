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
package com.google.cloud.teleport.v2.auto.dlq;

import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.ResourceUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.PCollection;

public class AutoDLQUtil {

  public static WriteResult writeDLQToBigQueryForPubsubMessage(
      PCollection<FailsafeElement<PubsubMessage, String>> input, String table) {
    return input.apply(
        ErrorConverters.WritePubsubMessageErrors.newBuilder()
            .setErrorRecordsTable(table)
            .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
            .build());
  }

  public static WriteResult writeDLQToBigQueryForString(
      PCollection<FailsafeElement<String, String>> input, String table) {
    return input.apply(
        ErrorConverters.WriteStringMessageErrors.newBuilder()
            .setErrorRecordsTable(table)
            .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
            .build());
  }
}
