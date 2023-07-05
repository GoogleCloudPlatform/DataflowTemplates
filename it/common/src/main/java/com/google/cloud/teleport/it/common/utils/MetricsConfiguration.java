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
package com.google.cloud.teleport.it.common.utils;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;

/** Utils for the metrics. */
@AutoValue
public abstract class MetricsConfiguration {

  /**
   * Input PCollection of the Dataflow job to query additional metrics. If not provided, the metrics
   * for inputPCollection will not be calculated.
   */
  public abstract @Nullable String inputPCollection();

  /**
   * Output PCollection of the Dataflow job to query additional metrics. If not provided, the
   * metrics for outputPCollection will not be calculated.
   */
  public abstract @Nullable String outputPCollection();

  @Nullable
  public abstract Function<List<Double>, List<Double>> seriesFilterFn();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract MetricsConfiguration.Builder setInputPCollection(@Nullable String value);

    public abstract MetricsConfiguration.Builder setOutputPCollection(@Nullable String value);

    public abstract MetricsConfiguration.Builder setSeriesFilterFn(
        @Nullable Function<List<Double>, List<Double>> value);

    public abstract MetricsConfiguration build();
  }

  public static MetricsConfiguration.Builder builder() {
    return new AutoValue_MetricsConfiguration.Builder();
  }

  /**
   * Calculate the average from a series.
   *
   * @param values the input series.
   * @param filterFn if non-null, a {@link Function} that filters the series.
   * @return the averaged result.
   */
  public static Double calculateAverage(
      List<Double> values, @Nullable Function<List<Double>, List<Double>> filterFn) {
    List<Double> filterValues = values;
    if (filterFn != null) {
      filterValues = filterFn.apply(values);
    }
    return filterValues.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  public static Double calculateAverage(List<Double> values) {
    return calculateAverage(values, null);
  }

  public static Function<List<Double>, List<Double>> filterBeginEndHalfAveFn() {
    return FilterBeginEndHalfAveFn.instance;
  }

  /**
   * A function that filters elements at the beginning and at the end, found by those smaller than
   * half of the maximum. Suitable when the throughput is steady.
   */
  public static class FilterBeginEndHalfAveFn implements Function<List<Double>, List<Double>> {

    static FilterBeginEndHalfAveFn instance = new FilterBeginEndHalfAveFn();

    private FilterBeginEndHalfAveFn() {}

    @Override
    public List<Double> apply(List<Double> inputs) {
      int seriesSize = inputs.size();
      if (seriesSize <= 1) {
        return inputs;
      }

      int fromIdx = 0;
      int toIdx = seriesSize - 1;
      double threshold = 0.5 * Collections.max(inputs);

      while (fromIdx < toIdx) {
        boolean advanced = false;
        if (inputs.get(fromIdx) < threshold) {
          fromIdx += 1;
          advanced = true;
        }
        if (inputs.get(toIdx) < threshold) {
          toIdx -= 1;
          advanced = true;
        }
        if (!advanced) {
          break;
        }
      }
      return inputs.subList(fromIdx, toIdx + 1);
    }
  }
}
