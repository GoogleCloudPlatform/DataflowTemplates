/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractPrimaryKeyFn
    extends DoFn<TrimmedShardedDataChangeRecord, KV<String, TrimmedShardedDataChangeRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(ExtractPrimaryKeyFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    TrimmedShardedDataChangeRecord record = c.element();
    try {
      String tableName = record.getTableName();
      String keysJson = record.getMod().getKeysJson();
      String key = tableName + "_" + keysJson;
      c.output(KV.of(key, record));
    } catch (Exception e) {
      LOG.error("Failed to extract primary key", e);
      throw new RuntimeException("Failed to extract primary key", e);
    }
  }
}
