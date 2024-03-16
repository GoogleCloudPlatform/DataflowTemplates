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
package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;

/** Test for transform {@link LineToRowFn}. */
public class LineToRowFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testTransform() {
    // Arrange
    Source source = new Source();
    Schema sourceSchema =
        Schema.of(Field.of("id", FieldType.STRING), Field.of("name", FieldType.STRING));
    CSVFormat csvFormat = CSVFormat.DEFAULT;

    // Act
    PCollection<Row> convertedRow =
        pipeline
            .apply(Create.of("1,Neo4j", "2,MySQL"))
            .apply(ParDo.of(new LineToRowFn(source, sourceSchema, csvFormat)))
            .setCoder(RowCoder.of(sourceSchema));

    // Assert
    Row neo4jRow =
        Row.withSchema(sourceSchema)
            .withFieldValue("id", "1")
            .withFieldValue("name", "Neo4j")
            .build();
    Row oracleRow =
        Row.withSchema(sourceSchema)
            .withFieldValue("id", "2")
            .withFieldValue("name", "MySQL")
            .build();
    PAssert.that(convertedRow).containsInAnyOrder(neo4jRow, oracleRow);

    pipeline.run();
  }

  @Test
  public void testNumberOfFieldsTruncated() {
    // Arrange
    Source source = new Source();
    Schema sourceSchema = Schema.of(Field.of("id", FieldType.STRING));
    CSVFormat csvFormat = CSVFormat.DEFAULT;

    // Act
    PCollection<Row> convertedRow =
        pipeline
            .apply(Create.of("1,Neo4j", "2,MySQL"))
            .apply(ParDo.of(new LineToRowFn(source, sourceSchema, csvFormat)))
            .setCoder(RowCoder.of(sourceSchema));

    // Assert
    Row neo4jRow = Row.withSchema(sourceSchema).withFieldValue("id", "1").build();
    Row oracleRow = Row.withSchema(sourceSchema).withFieldValue("id", "2").build();
    PAssert.that(convertedRow).containsInAnyOrder(neo4jRow, oracleRow);

    pipeline.run();
  }
}
