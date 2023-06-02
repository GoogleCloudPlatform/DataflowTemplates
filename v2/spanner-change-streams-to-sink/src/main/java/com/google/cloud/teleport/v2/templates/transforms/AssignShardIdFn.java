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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn assigns the shardId as key to the record. */
public class AssignShardIdFn
    extends DoFn<TrimmedDataChangeRecord, KV<String, TrimmedDataChangeRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignShardIdFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    TrimmedDataChangeRecord record = c.element();
    try {
      String keysJsonStr = record.getMods().get(0).getKeysJson();
      String newValueJsonStr = record.getMods().get(0).getNewValuesJson();
      JSONObject keysJson = new JSONObject(keysJsonStr);
      JSONObject newValueJson = new JSONObject(newValueJsonStr);
      if (newValueJson.has(Constants.HB_SHARD_ID_COLUMN)) {
        String shardId = newValueJson.getString(Constants.HB_SHARD_ID_COLUMN);
        c.output(KV.of(shardId, record));
      } else if (keysJson.has(Constants.HB_SHARD_ID_COLUMN)) {
        String shardId = keysJson.getString(Constants.HB_SHARD_ID_COLUMN);
        c.output(KV.of(shardId, record));
      } else {
        LOG.error(
            "Cannot find entry for HarbourBridge shard id column '"
                + Constants.HB_SHARD_ID_COLUMN
                + "' in record.");
        return;
      }
    } catch (Exception e) {
      LOG.error("Error while parsing json: " + e.getMessage());
      return;
    }
  }
}
