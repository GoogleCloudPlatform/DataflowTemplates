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
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The DataGeneratorSchemaTransform class is an implementation of the SchemaTransform that generates
 * testing data based on <a href="https://avro.apache.org/docs/1.10.2/spec.html#schemas">AVRO</a>
 * schema, data records per second and total running time in seconds. It can be directly used as
 * data source with urn: <b>xsyndeo:schematransform:com.google.cloud:data_generator:v1</b> in a
 * Syndeo pipeline as shown in following example:
 *
 * <pre>
 *    {
 *      "source": {
 *         "urn": "syndeo:schematransform:com.google.cloud:data_generator:v1",
 *         "configurationParameters": {
 *            "recordsPerSecond": 1000,
 *            "secondsToRun": 300,
 *            "schema": "test_schema.json"
 *          }
 *      }
 *    }
 * </pre>
 *
 * <p>AVRO schema example:
 *
 * <pre>
 *   {
 *   "type": "record",
 *   "name": "user_info_flat",
 *   "namespace": "com.google.syndeo",
 *   "fields": [
 *     {
 *       "name": "id",
 *       "type": "long"
 *     },
 *     {
 *       "name": "username",
 *       "type": "string",
 *       "size": "10"
 *     },
 *     {
 *       "name": "age",
 *       "type": "long"
 *     }
 *   ]
 * }
 * </pre>
 *
 * Random value will be generated for each field according to its type, for example, for a field of
 * type long, a random long value will be generated. For string field, random alpha-numeric string
 * will be generated. The default size of the string is 100, which can be specified using the
 * attribute <b>size</b>.
 */
public class DataGeneratorSchemaTransform extends SchemaTransform implements Serializable {

  private final long recordsPerSecond;
  private final long secondsToRun;
  private final String schema;

  public DataGeneratorSchemaTransform(long recordsPerSecond, long secondsToRun, String schema) {
    this.recordsPerSecond = recordsPerSecond;
    this.secondsToRun = secondsToRun;
    this.schema = schema;
  }

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
                    // set start time as current time
                    .startAt(startTime)
                    // set stop time
                    .stopAt(Instant.now().plus(Duration.standardSeconds(secondsToRun)))
                    // set data generating interval to 1 second
                    .withInterval(Duration.millis(1000))
                    .applyWindowing());
    instants = instants.apply(Reshuffle.viaRandomKey());

    // for each instant generated (every second), create data records.
    PCollection rows =
        instants
            .apply(FlatMapElements.via(new InstantToRowFn(startTime, schema, beamSchema)))
            .setRowSchema(beamSchema);
    return PCollectionRowTuple.of("output", rows);
  }

  private class InstantToRowFn extends SimpleFunction<Instant, List<Row>> {

    private final Instant startTime;

    /**
     * The avro schema string. This is needed to pass addition attributes like the string field size
     * and a list of values for a field. TODO: Once implementing support for passing additional
     * attributes in Beam Schema, avroSchemaString can be removed.
     */
    private final String avroSchemaString;

    private final org.apache.beam.sdk.schemas.Schema beamSchema;

    public InstantToRowFn(
        Instant startTime, String avroSchemaString, org.apache.beam.sdk.schemas.Schema beamSchema) {
      this.startTime = startTime;
      this.avroSchemaString = avroSchemaString;
      this.beamSchema = beamSchema;
    }

    @Override
    public List<Row> apply(Instant input) {
      final Schema schema = Schema.parse(this.avroSchemaString);
      long ordinal = (input.minus(Duration.millis(startTime.getMillis())).getMillis() / 1000);
      return LongStream.range(ordinal * recordsPerSecond, (ordinal + 1) * recordsPerSecond)
          .mapToObj(intVal -> RecordCreator.createRowRecord(schema, beamSchema))
          .collect(Collectors.toList());
    }
  }
}
