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
package com.google.cloud.syndeo.transforms;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class SyndeoStatsSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SyndeoStatsSchemaTransformProvider.SyndeoStatsConfiguration> {

  @Override
  public Class<SyndeoStatsConfiguration> configurationClass() {
    return SyndeoStatsConfiguration.class;
  }

  @Override
  public SchemaTransform from(SyndeoStatsConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            PCollection<Row> inputPcoll = input.get("input");
            return PCollectionRowTuple.of(
                "output",
                inputPcoll
                    .apply(
                        String.format("%s_%s", configuration.getParent(), identifier()),
                        ParDo.of(new StatsDoFn()))
                    .setRowSchema(inputPcoll.getSchema()));
          }
        };
      }
    };
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "syndeo:schematransform:com.google.cloud:syndeo_stats:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SyndeoStatsConfiguration {
    public abstract String getParent();

    public static SyndeoStatsConfiguration create(String parent) {
      return new AutoValue_SyndeoStatsSchemaTransformProvider_SyndeoStatsConfiguration(parent);
    }
  }

  private static class StatsDoFn extends DoFn<Row, Row> {
    private Long elementsProcessed = 0L;
    final Counter elementsCounter =
        Metrics.counter(SyndeoStatsSchemaTransformProvider.class, "elementsProcessed");

    @DoFn.ProcessElement
    public void process(@DoFn.Element Row elm, OutputReceiver<Row> receiver) {
      elementsProcessed += 1;
      receiver.output(elm);
    }

    @DoFn.FinishBundle
    public void finish() {
      elementsCounter.inc(elementsProcessed);
    }
  }

  public static Long getElementsProcessed(MetricResults jobMetrics) {
    try {
      return jobMetrics
          .queryMetrics(
              MetricsFilter.builder()
                  .addNameFilter(
                      MetricNameFilter.named(
                          SyndeoStatsSchemaTransformProvider.class, "elementsProcessed"))
                  .build())
          .getCounters()
          .iterator()
          .next()
          .getCommitted();
    } catch (NoSuchElementException e) {
      System.out.println("Unable to find elementsProcessed counter for job.");
      return 0L;
    }
  }
}
