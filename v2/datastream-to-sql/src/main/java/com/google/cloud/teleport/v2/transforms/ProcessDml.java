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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.values.DmlInfo;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ProcessDml} class statefully processes and filters data based on the supplied primary
 * keys and sort keys in DmlInfo.
 */
public class ProcessDml {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessDml.class);
  private static final String WINDOW_DURATION = "1s";
  private static final int NUM_THREADS = 8;

  public ProcessDml() {}

  public static StatefulProcessDml statefulOrderByPK() {
    return new StatefulProcessDml();
  }

  /** This class is used as the default return value of {@link ProcessDml#statefulOrderByPK()}. */
  public static class StatefulProcessDml
      extends PTransform<PCollection<KV<String, DmlInfo>>, PCollection<KV<String, DmlInfo>>> {

    public StatefulProcessDml() {}

    @Override
    public PCollection<KV<String, DmlInfo>> expand(PCollection<KV<String, DmlInfo>> input) {
      return input
          .apply(ParDo.of(new StatefulProcessDmlFn()))
          .apply(
              "Creating " + WINDOW_DURATION + " Window",
              Window.into(FixedWindows.of(DurationUtils.parseDuration(WINDOW_DURATION))))
          .apply(Reshuffle.of());
    }
  }

  /**
   * The {@code StatefulProcessDmlFn} class statefully processes and filters data based on the
   * supplied primary keys and sort keys in DmlInfo.
   */
  public static class StatefulProcessDmlFn extends DoFn<KV<String, DmlInfo>, KV<String, DmlInfo>> {

    private static final String PK_STATE_ID = "pk-state-id";
    private final Distribution distribution =
        Metrics.distribution(StatefulProcessDmlFn.class, "replicationDistribution");

    @StateId(PK_STATE_ID)
    private final StateSpec<ValueState<String>> myStateSpec =
        StateSpecs.value(StringUtf8Coder.of());

    public StatefulProcessDmlFn() {}

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId(PK_STATE_ID) ValueState<String> myState) {
      String stateKey = context.element().getKey();
      DmlInfo dmlInfo = context.element().getValue();
      // Empty SQL suggests the table does not exist and should be skipped
      if (dmlInfo.getDmlSql().equals("")) {
        return;
      }

      // TODO(dhercher): More complex compare w/o String.join
      String lastSortKey = myState.read();
      String currentSortKey = dmlInfo.getOrderByValueString();

      // If there is no PK then state can be skipped
      if (dmlInfo.getAllPkFields().size() == 0) {
        String numThreads = Integer.toString(stateKey.hashCode() % NUM_THREADS);
        context.output(KV.of(numThreads, dmlInfo));
      } else if (lastSortKey == null || currentSortKey.compareTo(lastSortKey) > 0) {
        String numThreads = Integer.toString(stateKey.hashCode() % NUM_THREADS);
        myState.write(currentSortKey);
        context.output(KV.of(numThreads, dmlInfo));

        distribution.update(0);
      }
    }
  }
}
