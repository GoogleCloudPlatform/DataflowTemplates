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
package com.google.cloud.syndeo.perf;

import java.math.BigDecimal;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class SyndeoLoadTestUtils {

  public static PCollection<Row> inputData(
      Pipeline dataGenerator, Long numRows, Integer runtimeMinutes) {
    Random randomSeed = new Random();
    // TODO(pabloem): Determine rate of production of data.
    return dataGenerator
        .apply(
            GenerateSequence.from(0)
                .to(numRows)
                .withRate(numRows / runtimeMinutes / 60, Duration.standardSeconds(1)))
        .apply(Reshuffle.viaRandomKey())
        .apply(
            MapElements.into(TypeDescriptors.rows())
                .via(
                    longVal ->
                        SyndeoLoadTestUtils.randomRowForSchema(
                            SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA, 0.05, randomSeed)))
        .setRowSchema(SIMPLE_TABLE_SCHEMA);
  }

  // A schema for a table that has been slightly inspired on the Github public dataset
  // provided by BigQuery.
  public static final Schema SIMPLE_TABLE_SCHEMA =
      Schema.builder()
          .addField(Schema.Field.nullable("commit", Schema.FieldType.STRING))
          .addField(Schema.Field.nullable("repo_name", Schema.FieldType.STRING))
          .addField(Schema.Field.of("parent", Schema.FieldType.array(Schema.FieldType.STRING)))
          .addField(Schema.Field.of("commitDate", Schema.FieldType.DATETIME))
          .addField(Schema.Field.nullable("message", Schema.FieldType.STRING))
          .addInt64Field("linesAdded")
          .addInt32Field("linesRemoved")
          .addBooleanField("merged")
          .addByteArrayField("sha1")
          // A decimal field that means nothing but that we include for good measure : )
          .addDecimalField("decimalForGoodMeasure")
          .build();

  public static final Schema NESTED_TABLE_SCHEMA =
      Schema.builder()
          .addFields(SIMPLE_TABLE_SCHEMA.getFields())
          .addField(
              Schema.Field.nullable(
                  "author",
                  Schema.FieldType.row(
                      Schema.builder()
                          .addField(Schema.Field.of("name", Schema.FieldType.STRING))
                          .addField(Schema.Field.of("email", Schema.FieldType.STRING))
                          .build())))
          .build();

  public static String randomString(Integer length, Random randomSeed) {
    return randomSeed
        .ints(length, 48, 122)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  private static <T> T generateOrNull(
      SerializableFunction<Void, T> generator, double nullProbability, Random randomSeed) {
    if (randomSeed.nextDouble() > nullProbability) {
      return generator.apply(null);
    } else {
      return null;
    }
  }

  public static Row randomRowForSchema(
      Schema inputSchema, double nullProbability, Random randomSeed) {
    Row.FieldValueBuilder rowBuilder =
        Row.withSchema(inputSchema).withFieldValue(inputSchema.getField(0).getName(), "any");
    //    Row.FieldValueBuilder fieldValueBuilder = null;
    for (Schema.Field f : inputSchema.getFields()) {
      switch (f.getType().getTypeName()) {
        case STRING:
          String str =
              generateOrNull(
                  ignored -> randomString(50, randomSeed),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (str == null) continue;
          rowBuilder.withFieldValue(f.getName(), str);
          break;
        case DECIMAL:
          BigDecimal bigDecimal =
              generateOrNull(
                  ignored -> new BigDecimal(String.valueOf(randomSeed.nextLong())),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (bigDecimal == null) continue;
          rowBuilder.withFieldValue(f.getName(), bigDecimal);
          break;
        case INT32:
        case INT16:
          Integer theInt =
              generateOrNull(
                  ignored -> randomSeed.nextInt(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theInt == null) continue;
          rowBuilder.withFieldValue(f.getName(), theInt);
          break;
        case INT64:
          Long theLong =
              generateOrNull(
                  ignored -> randomSeed.nextLong(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theLong == null) continue;
          rowBuilder.withFieldValue(f.getName(), theLong);
          break;
        case BYTES:
          // Bytes always non-null
          byte[] theBytes = new byte[50];
          randomSeed.nextBytes(theBytes);
          rowBuilder.withFieldValue(f.getName(), theBytes);
          break;
        case FLOAT:
          Float theFloat =
              generateOrNull(
                  ignored -> randomSeed.nextFloat(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theFloat == null) continue;
          rowBuilder.withFieldValue(f.getName(), theFloat);
          break;
        case DOUBLE:
          Double theDouble =
              generateOrNull(
                  ignored -> randomSeed.nextDouble(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theDouble == null) continue;
          rowBuilder.withFieldValue(f.getName(), theDouble);
          break;
        case ROW:
          rowBuilder.withFieldValue(
              f.getName(), randomRowForSchema(f.getType().getRowSchema(), 0, randomSeed));
          break;
        case BOOLEAN:
          Boolean theBool =
              generateOrNull(
                  ignored -> randomSeed.nextBoolean(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theBool == null) continue;
          rowBuilder.withFieldValue(f.getName(), theBool);
          break;
        case DATETIME:
          rowBuilder.withFieldValue(f.getName(), new DateTime(randomSeed.nextLong()));
          break;
        case ARRAY:
        case ITERABLE:
          rowBuilder.withFieldValue(
              f.getName(),
              IntStream.range(0, 10)
                  .mapToObj(
                      inty ->
                          randomRowForSchema(
                              Schema.builder()
                                  .addField("main", f.getType().getCollectionElementType())
                                  .build(),
                              0,
                              randomSeed))
                  .map(row -> row.getValue("main"))
                  .collect(Collectors.toList()));
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unable to generate field with type %s", f.getType()));
      }
    }
    return rowBuilder.build();
  }
}
