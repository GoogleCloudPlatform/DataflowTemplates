/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic.dofns;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * The InjectKeysFn associates a numeric Key, between 0 (inclusive) and "specifiedParallelism"
 * (exclusive), to each of the {@link NewRelicLogRecord}s it processes. This will effectively
 * distribute the processing of such log records (in a multi-worker cluster), since all the log
 * records having the same key will be processed by the same worker.
 */
public class InjectKeysFn extends DoFn<NewRelicLogRecord, KV<Integer, NewRelicLogRecord>> {

  private final ValueProvider<Integer> specifiedParallelism;

  public InjectKeysFn(ValueProvider<Integer> specifiedParallelism) {
    this.specifiedParallelism = specifiedParallelism;
  }

  @ProcessElement
  public void processElement(
      @Element NewRelicLogRecord inputElement,
      OutputReceiver<KV<Integer, NewRelicLogRecord>> outputReceiver) {
    outputReceiver.output(
        KV.of(ThreadLocalRandom.current().nextInt(specifiedParallelism.get()), inputElement));
  }
}
