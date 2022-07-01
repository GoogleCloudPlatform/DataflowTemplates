/*
 * Copyright (C) 2022 Google LLC
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

import com.google.common.base.MoreObjects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create KvTransform to control Beam parallelism. */
public class CreateKvTransform extends PTransform<PCollection<Row>, PCollection<KV<Integer, Row>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateKvTransform.class);
  private static final Integer DEFAULT_PARALLELISM = 1;
  private final Integer requestedKeys;

  private CreateKvTransform(Integer requestedKeys) {
    this.requestedKeys = requestedKeys;
  }

  public static CreateKvTransform of(Integer requestedKeys) {
    return new CreateKvTransform(requestedKeys);
  }

  @Override
  public PCollection<KV<Integer, Row>> expand(PCollection<Row> input) {
    return input
        .apply("Inject Keys", ParDo.of(new CreateKeysFn(this.requestedKeys)))
        .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), input.getCoder()));
  }

  private class CreateKeysFn extends DoFn<Row, KV<Integer, Row>> {
    private final Integer specifiedParallelism;
    private Integer calculatedParallelism;

    CreateKeysFn(Integer specifiedParallelism) {
      this.specifiedParallelism = specifiedParallelism;
    }

    @Setup
    public void setup() {

      if (calculatedParallelism == null) {

        if (specifiedParallelism != null) {
          calculatedParallelism = specifiedParallelism;
        }

        calculatedParallelism =
            MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

        LOG.info("Parallelism set to: {}", calculatedParallelism);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
    }
  }
}
