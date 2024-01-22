/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.BigtableToVectorEmbeddings.BigtableToVectorEmbeddingsFn;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.bigtable.v2.Row;
import com.google.cloud.teleport.bigtable.BigtableToVectorEmbeddings.BigtableToVectorEmbeddingsFn;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigtableToVectorEmbeddings}. */
@RunWith(JUnit4.class)
public final class BigtableToVectorEmbeddingsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBigtableToVectorEmbeddings_float() throws Exception {
    String expectedJson =
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"cat\"]},{\"namespace\":\"color\",\"deny\":[\"white\"]}],\"numeric_restricts\":[{\"namespace\":\"some_int_value\",\"value_int\":5},{\"namespace\":\"some_float_value\",\"value_float\":3.14},{\"namespace\":\"some_double_value\",\"value_double\":2.71}]}";
    Row row = createBigtableRow("1");
    row =
        upsertBigtableCell(
            row,
            "cf1",
            "embedding",
            1,
            ByteString.copyFrom(ArrayUtils.addAll(Bytes.toBytes(3.14f), Bytes.toBytes(2.71f))));
    row = upsertBigtableCell(row, "cf2", "crowding", 1, "crowd1");
    row = upsertBigtableCell(row, "cf", "animal", 1, "cat");
    row = upsertBigtableCell(row, "cf", "color", 1, "white");
    row = upsertBigtableCell(row, "cf", "some_int_value", 1, ByteString.copyFrom(Bytes.toBytes(5)));
    row =
        upsertBigtableCell(
            row, "cf", "some_float_value", 1, ByteString.copyFrom(Bytes.toBytes(3.14f)));
    row =
        upsertBigtableCell(
            row, "cf", "some_double_value", 1, ByteString.copyFrom(Bytes.toBytes(2.71d)));

    PCollection<String> jsonRows =
        pipeline
            .apply("Create", Create.of(row))
            .apply(
                "Transform to JSON",
                MapElements.via(
                    new BigtableToVectorEmbeddingsFn(
                        StaticValueProvider.of("row_key"),
                        StaticValueProvider.of("cf1:embedding"),
                        StaticValueProvider.of(4),
                        StaticValueProvider.of("cf2:crowding"),
                        StaticValueProvider.of("cf:animal;animal"),
                        StaticValueProvider.of("cf:color;color"),
                        StaticValueProvider.of("cf:some_int_value;some_int_value"),
                        StaticValueProvider.of("cf:some_float_value;some_float_value"),
                        StaticValueProvider.of("cf:some_double_value;some_double_value"))))
            .setCoder(StringUtf8Coder.of());

    // Assert on the jsonRows.
    PAssert.that(jsonRows).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  @Test
  public void testBigtableToVectorEmbeddings_double() throws Exception {
    String expectedJson =
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"cat\"]},{\"namespace\":\"color\",\"deny\":[\"white\"]}],\"numeric_restricts\":[{\"namespace\":\"some_int_value\",\"value_int\":5},{\"namespace\":\"some_float_value\",\"value_float\":3.14},{\"namespace\":\"some_double_value\",\"value_double\":2.71}]}";
    Row row = createBigtableRow("1");
    row =
        upsertBigtableCell(
            row,
            "cf1",
            "embedding",
            1,
            ByteString.copyFrom(ArrayUtils.addAll(Bytes.toBytes(3.14d), Bytes.toBytes(2.71d))));
    row = upsertBigtableCell(row, "cf2", "crowding", 1, "crowd1");
    row = upsertBigtableCell(row, "cf", "animal", 1, "cat");
    row = upsertBigtableCell(row, "cf", "color", 1, "white");
    row = upsertBigtableCell(row, "cf", "some_int_value", 1, ByteString.copyFrom(Bytes.toBytes(5)));
    row =
        upsertBigtableCell(
            row, "cf", "some_float_value", 1, ByteString.copyFrom(Bytes.toBytes(3.14f)));
    row =
        upsertBigtableCell(
            row, "cf", "some_double_value", 1, ByteString.copyFrom(Bytes.toBytes(2.71d)));

    PCollection<String> jsonRows =
        pipeline
            .apply("Create", Create.of(row))
            .apply(
                "Transform to JSON",
                MapElements.via(
                    new BigtableToVectorEmbeddingsFn(
                        StaticValueProvider.of("row_key"),
                        StaticValueProvider.of("cf1:embedding"),
                        StaticValueProvider.of(8),
                        StaticValueProvider.of("cf2:crowding"),
                        StaticValueProvider.of("cf:animal;animal"),
                        StaticValueProvider.of("cf:color;color"),
                        StaticValueProvider.of("cf:some_int_value;some_int_value"),
                        StaticValueProvider.of("cf:some_float_value;some_float_value"),
                        StaticValueProvider.of("cf:some_double_value;some_double_value"))))
            .setCoder(StringUtf8Coder.of());

    // Assert on the jsonRows.
    PAssert.that(jsonRows).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  @Test
  public void testBigtableToVectorEmbeddings_restricts() throws Exception {
    // Restricts
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"cat\"]}]}",
        "cf:animal;animal",
        "",
        "",
        "",
        "");
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"color\",\"deny\":[\"white\"]}]}",
        "",
        "cf:color;color",
        "",
        "",
        "");
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\"}", "", "", "", "", "");

    // Numeric restricts
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"numeric_restricts\":[{\"namespace\":\"some_int_value\",\"value_int\":5}]}",
        "",
        "",
        "cf:some_int_value;some_int_value",
        "",
        "");
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"numeric_restricts\":[{\"namespace\":\"some_float_value\",\"value_float\":3.14}]}",
        "",
        "",
        "",
        "cf:some_float_value;some_float_value",
        "");
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\",\"numeric_restricts\":[{\"namespace\":\"some_double_value\",\"value_double\":2.71}]}",
        "",
        "",
        "",
        "",
        "cf:some_double_value;some_double_value");
    testBigtableToVectorEmbeddings_restricts(
        "{\"id\":\"1\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd1\"}", "", "", "", "", "");
  }

  private void testBigtableToVectorEmbeddings_restricts(
      String expectedJson,
      String allow,
      String deny,
      String intRestricts,
      String floatRestricts,
      String doubleRestricts)
      throws Exception {
    Row row = createBigtableRow("1");
    row =
        upsertBigtableCell(
            row,
            "cf1",
            "embedding",
            1,
            ByteString.copyFrom(ArrayUtils.addAll(Bytes.toBytes(3.14d), Bytes.toBytes(2.71d))));
    row = upsertBigtableCell(row, "cf2", "crowding", 1, "crowd1");
    row = upsertBigtableCell(row, "cf", "animal", 1, "cat");
    row = upsertBigtableCell(row, "cf", "color", 1, "white");
    row = upsertBigtableCell(row, "cf", "some_int_value", 1, ByteString.copyFrom(Bytes.toBytes(5)));
    row =
        upsertBigtableCell(
            row, "cf", "some_float_value", 1, ByteString.copyFrom(Bytes.toBytes(3.14f)));
    row =
        upsertBigtableCell(
            row, "cf", "some_double_value", 1, ByteString.copyFrom(Bytes.toBytes(2.71d)));

    PCollection<String> jsonRows =
        pipeline
            .apply("Create", Create.of(row))
            .apply(
                "Transform to JSON",
                MapElements.via(
                    new BigtableToVectorEmbeddingsFn(
                        StaticValueProvider.of("row_key"),
                        StaticValueProvider.of("cf1:embedding"),
                        StaticValueProvider.of(8),
                        StaticValueProvider.of("cf2:crowding"),
                        StaticValueProvider.of(allow),
                        StaticValueProvider.of(deny),
                        StaticValueProvider.of(intRestricts),
                        StaticValueProvider.of(floatRestricts),
                        StaticValueProvider.of(doubleRestricts))))
            .setCoder(StringUtf8Coder.of());

    // Assert on the jsonRows.
    PAssert.that(jsonRows).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  @Test
  public void testBigtableToVectorEmbeddings_missingIdValue() throws Exception {
    Row row = createBigtableRow("1");
    row =
        upsertBigtableCell(
            row,
            "cf1",
            "embedding",
            1,
            ByteString.copyFrom(ArrayUtils.addAll(Bytes.toBytes(3.14d), Bytes.toBytes(2.71d))));

    pipeline
        .apply("Create", Create.of(row))
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToVectorEmbeddingsFn(
                    StaticValueProvider.of("cf:nope"),
                    StaticValueProvider.of("cf1:embedding"),
                    StaticValueProvider.of(8),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""))))
        .setCoder(StringUtf8Coder.of());

    // Assert error.
    Exception thrown = assertThrows(RuntimeException.class, () -> pipeline.run());

    assertThat(thrown).hasMessageThat().contains("'id' value is missing for row '1'");
  }

  @Test
  public void testBigtableToVectorEmbeddings_missingDoubleEmbeddingValue() throws Exception {
    Row row = createBigtableRow("1");

    pipeline
        .apply("Create", Create.of(row))
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToVectorEmbeddingsFn(
                    StaticValueProvider.of("row_key"),
                    StaticValueProvider.of("cf1:embedding"),
                    StaticValueProvider.of(8),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""))))
        .setCoder(StringUtf8Coder.of());

    // Assert error.
    Exception thrown = assertThrows(RuntimeException.class, () -> pipeline.run());

    assertThat(thrown).hasMessageThat().contains("'embedding' value is missing for row '1'");
  }

  @Test
  public void testBigtableToVectorEmbeddings_missingFloatEmbeddingValue() throws Exception {
    Row row = createBigtableRow("1");

    pipeline
        .apply("Create", Create.of(row))
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToVectorEmbeddingsFn(
                    StaticValueProvider.of("row_key"),
                    StaticValueProvider.of("cf1:embedding"),
                    StaticValueProvider.of(4),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""))))
        .setCoder(StringUtf8Coder.of());

    // Assert error.
    Exception thrown = assertThrows(RuntimeException.class, () -> pipeline.run());

    assertThat(thrown).hasMessageThat().contains("'embedding' value is missing for row '1'");
  }

  @Test
  public void testBigtableToVectorEmbeddings_invalidEmbeddingByteSize() throws Exception {
    Row row = createBigtableRow("1");

    pipeline
        .apply("Create", Create.of(row))
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToVectorEmbeddingsFn(
                    StaticValueProvider.of("row_key"),
                    StaticValueProvider.of("cf1:embedding"),
                    StaticValueProvider.of(5),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of(""))))
        .setCoder(StringUtf8Coder.of());

    // Assert error.
    Exception thrown = assertThrows(RuntimeException.class, () -> pipeline.run());

    assertThat(thrown).hasMessageThat().contains("embeddingByteSize can be either 4 or 8");
  }
}
