/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SplitIntoBatches<T> extends DoFn<KV<Integer, Iterable<T>>, KV<Integer, Iterable<T>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateKvTransform.class);
  private final int batchSize;

  private SplitIntoBatches(int batchSize) {
    this.batchSize = batchSize;
  }

  public static <T> SplitIntoBatches<T> of(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException(
          String.format("negative batch sizes are not permitted, %d given", batchSize));
    }
    return new SplitIntoBatches<>(batchSize);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    LOG.info("Splitting into batches of {} rows", batchSize);
    KV<Integer, Iterable<T>> input = context.element();
    Integer key = input.getKey();
    Partitioner.partition(input.getValue(), batchSize)
        .forEach(batch -> context.output(KV.of(key, batch)));
  }
}
