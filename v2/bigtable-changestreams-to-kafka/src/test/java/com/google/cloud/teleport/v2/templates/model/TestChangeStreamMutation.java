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
package com.google.cloud.teleport.v2.templates.model;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.time.Instant;

public class TestChangeStreamMutation extends ChangeStreamMutation {
  private final Entry entry;
  private final Instant commitTime = Instant.parse("2026-01-01T00:00:00.123456Z");

  public TestChangeStreamMutation(Entry entry) {
    this.entry = entry;
  }

  @Override
  public ByteString getRowKey() {
    return ByteString.copyFromUtf8("row");
  }

  @Override
  public MutationType getType() {
    return MutationType.USER;
  }

  @Override
  public String getSourceClusterId() {
    return "cluster";
  }

  @Override
  public Instant getCommitTime() {
    return commitTime;
  }

  @Override
  public int getTieBreaker() {
    return 0;
  }

  @Override
  public String getToken() {
    return "token";
  }

  @Override
  public Instant getEstimatedLowWatermarkTime() {
    return commitTime;
  }

  @Override
  public ImmutableList<Entry> getEntries() {
    return ImmutableList.of(entry);
  }
}
