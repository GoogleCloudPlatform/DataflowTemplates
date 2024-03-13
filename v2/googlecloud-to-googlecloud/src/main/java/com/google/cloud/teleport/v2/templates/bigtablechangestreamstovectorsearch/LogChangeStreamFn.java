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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogChangeStreamFn
    extends DoFn<KV<ByteString, ChangeStreamMutation>, KV<ByteString, ChangeStreamMutation>> {

  // @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
  private static final Logger LOG = LoggerFactory.getLogger(LogChangeStreamFn.class);

  @ProcessElement
  public void processElement(
      @Element KV<ByteString, ChangeStreamMutation> input,
      OutputReceiver<KV<ByteString, ChangeStreamMutation>> out)
      throws Exception {
    LOG.info("Received Change:");

    ChangeStreamMutation mutation = input.getValue();

    LOG.info("ChangeStreamMutation:");
    LOG.info("  - rowkey: {}", mutation.getRowKey().toStringUtf8());
    LOG.info("  - type: {}", mutation.getType());
    LOG.info("  - Mods:");
    for (Entry entry : mutation.getEntries()) {
      LOG.info("    - mod: {}", entry);
      LOG.info("    - class: {}", entry.getClass());
      if (entry instanceof SetCell) {
        LOG.info("    - type: SetCell");
        SetCell m = (SetCell) entry;
        LOG.info("    - familyName: {}", m.getFamilyName());
        LOG.info("    - qualifier: {}", m.getQualifier().toStringUtf8());
        LOG.info("    - timestamp: {}", m.getTimestamp());
        LOG.info("    - value: {}", m.getValue().toByteArray());
      } else if (entry instanceof DeleteCells) {
        LOG.info("    - type: DeleteCell");
        DeleteCells m = (DeleteCells) entry;
        LOG.info("    - familyName: {}", m.getFamilyName());
        LOG.info("    - qualifier: {}", m.getQualifier().toStringUtf8());
        LOG.info("    - timestamp: {}", m.getTimestampRange());
      } else if (entry instanceof DeleteFamily) {
        LOG.info("    - type: DeleteFamily");
        DeleteFamily m = (DeleteFamily) entry;
        LOG.info("    - familyName: {}", m.getFamilyName());
      } else {
        LOG.warn("Unexpected mod type");
      }
    }

    out.output(input);
  }
}
