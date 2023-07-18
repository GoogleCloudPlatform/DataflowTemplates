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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.SimpleFunction;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.bigtable.BigtableRow;

/**
 * A {@link org.apache.beam.sdk.transforms.PTransform} which converts items in a {@link
 * org.apache.beam.sdk.values.PCollection} from {@link
 * com.google.cloud.teleport.bigtable.ChangelogEntry} to {@link
 * com.google.cloud.teleport.bigtable.BigtableRow}.
 */
public class BigtableChangelogEntryToBigtableRowFn
    extends SimpleFunction<ChangelogEntry, BigtableRow> {

  private final String workerId;
  private final AtomicLong counter;
  private final BigtableUtils bigtableUtils;

  public BigtableChangelogEntryToBigtableRowFn(
      String workerId, AtomicLong counter, BigtableUtils bigtableUtils) {
    this.workerId = workerId;
    this.counter = counter;
    this.bigtableUtils = bigtableUtils;
  }

  @Override
  public BigtableRow apply(ChangelogEntry entry) {
    return this.bigtableUtils.createBigtableRow(entry, workerId, counter.incrementAndGet());
  }
}
