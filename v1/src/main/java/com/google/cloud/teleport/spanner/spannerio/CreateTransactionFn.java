/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.TimestampBound;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Creates a batch transaction.
 *
 * <p>WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class CreateTransactionFn
    extends DoFn<Object, com.google.cloud.teleport.spanner.spannerio.Transaction> {
  private final SpannerConfig config;
  private final TimestampBound timestampBound;

  CreateTransactionFn(SpannerConfig config, TimestampBound timestampBound) {
    this.config = config;
    this.timestampBound = timestampBound;
  }

  private transient com.google.cloud.teleport.spanner.spannerio.SpannerAccessor spannerAccessor;

  @Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(config);
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    BatchReadOnlyTransaction tx =
        spannerAccessor.getBatchClient().batchReadOnlyTransaction(timestampBound);
    c.output(Transaction.create(tx.getBatchTransactionId()));
  }
}
