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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PTransform} that breaks fusion to improve parallelism in the data extraction path.
 *
 * <p><b>Fusion Breaking Mechanism:</b>
 *
 * <p>This transform achieves a fusion break by passing the main input through an identity {@link
 * DoFn} that consumes a side-input. In Dataflow, consuming a side-input forces a materialization
 * point, effectively preventing the runner from fusing upstream operations (like the JDBC read)
 * with downstream operations (like Spanner mutations).
 *
 * <p><b>Timestamp Side Effect:</b>
 *
 * <p>Because this fusion break uses a side-input generated in the {@link
 * org.apache.beam.sdk.transforms.windowing.GlobalWindow}, all output elements inadvertently inherit
 * a timestamp of {@code TIMESTAMP_MAX_VALUE}. To correct this, this transform applies {@link
 * TimeStampRow} as a final step to reset the timestamps back to the current system time, ensuring
 * compatibility with downstream components like the Dead Letter Queue (DLQ).
 *
 * @param <T> The type of the element being processed.
 */
public class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
  @Override
  public PCollection<T> expand(PCollection<T> input) {
    // See https://issues.apache.org/jira/browse/BEAM-2803
    // We use a combined approach to "break fusion" here:
    // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
    // 1) force the data to be materialized by passing it as a side input to an identity fn,
    // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
    // and ensures that data to be shuffled can be generated in parallel, while reshuffling
    // provides perfect parallelism.
    // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
    // The current approach is necessary only to support the particular case of JdbcIO where
    // a single query may produce many gigabytes of query results.
    PCollectionView<Iterable<T>> empty =
        input
            .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
            .apply(View.asIterable());
    PCollection<T> materialized =
        input.apply(
            "Identity",
            ParDo.of(
                    new DoFn<T, T>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        c.output(c.element());
                      }
                    })
                .withSideInputs(empty));
    return materialized.apply(Reshuffle.viaRandomKey());
  }
}
