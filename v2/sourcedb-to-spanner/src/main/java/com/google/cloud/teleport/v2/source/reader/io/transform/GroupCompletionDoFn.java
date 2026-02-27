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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * DoFn to map the aggregated record counts for each table back to their corresponding {@link
 * SourceTableReference} and output them with the updated count.
 */
class GroupCompletionDoFn extends DoFn<KV<String, Long>, SourceTableReference> {
  private final Map<String, SourceTableReference> tableReferencesMap;

  public GroupCompletionDoFn(ImmutableList<SourceTableReference> tableReferences) {
    this.tableReferencesMap =
        tableReferences.stream()
            .collect(Collectors.toMap(SourceTableReference::sourceTableName, ref -> ref));
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, Long> element, OutputReceiver<SourceTableReference> out) {
    SourceTableReference ref = tableReferencesMap.get(element.getKey());
    if (ref != null) {
      out.output(ref.toBuilder().setRecordCount(element.getValue()).build());
    }
  }
}
