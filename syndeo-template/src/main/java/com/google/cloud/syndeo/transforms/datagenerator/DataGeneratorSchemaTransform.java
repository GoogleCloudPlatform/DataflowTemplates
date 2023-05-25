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
package com.google.cloud.syndeo.transforms.datagenerator;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class DataGeneratorSchemaTransform implements SchemaTransform, Serializable {
  private final long recordsPerSecond;
  private final long secondsToRun;
  private final String schema;

  public DataGeneratorSchemaTransform(long recordsPerSecond, long secondsToRun, String schema) {
    this.recordsPerSecond = recordsPerSecond;
    this.secondsToRun = secondsToRun;
    this.schema = schema;
  }

  @Override
  public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
    return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        org.apache.beam.sdk.schemas.Schema beamSchema =
            AvroUtils.toBeamSchema(new Parser().parse(schema));
        final Instant startTime = Instant.now();
        PCollection<Instant> instants =
            input
                .getPipeline()
                .apply(
                    PeriodicImpulse.create()
                        .startAt(startTime)
                        .stopAt(Instant.now().plus(Duration.standardSeconds(secondsToRun)))
                        .withInterval(Duration.millis(1000))
                        .applyWindowing());
        instants = instants.apply(Reshuffle.viaRandomKey());

        PCollection rows =
            instants
                .apply(FlatMapElements.via(new InstantToRowFn(startTime, schema)))
                .setRowSchema(beamSchema);
        return PCollectionRowTuple.of("output", rows);
      }
    };
  }

  private class InstantToRowFn extends SimpleFunction<Instant, List<Row>> {

    private final Instant startTime;
    private final String schema;

    public InstantToRowFn(Instant startTime, String schema) {
      this.startTime = startTime;
      this.schema = schema;
    }

    @Override
    public List<Row> apply(Instant input) {
      final Schema schema = Schema.parse(this.schema);
      long ordinal = (input.minus(Duration.millis(startTime.getMillis())).getMillis() / 1000);
      return LongStream.range(ordinal * recordsPerSecond, (ordinal + 1) * recordsPerSecond)
          .mapToObj(intVal -> RecordCreator.createRowRecord(schema))
          .collect(Collectors.toList());
    }
  }
}
