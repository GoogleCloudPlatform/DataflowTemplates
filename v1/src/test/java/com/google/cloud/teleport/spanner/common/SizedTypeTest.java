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
package com.google.cloud.teleport.spanner.common;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type.StructField;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SizedTypeTest {

  @Test
  public void testStruct() {
    SizedType simpleStruct =
        SizedType.parseSpannerType("STRUCT<a BOOL>", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(
        simpleStruct.type, Type.struct(ImmutableList.of(StructField.of("a", Type.bool()))));
    assertEquals(SizedType.typeString(simpleStruct.type, null), "STRUCT<a BOOL>");

    SizedType nestedStruct =
        SizedType.parseSpannerType("STRUCT<a STRUCT<b BOOL>>", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(
        nestedStruct.type,
        Type.struct(
            ImmutableList.of(
                StructField.of(
                    "a", Type.struct(ImmutableList.of(StructField.of("b", Type.bool())))))));
    assertEquals(SizedType.typeString(nestedStruct.type, null), "STRUCT<a STRUCT<b BOOL>>");

    SizedType complexStruct =
        SizedType.parseSpannerType(
            "STRUCT<a BOOL, b ARRAY<STRUCT<c STRING(MAX), d ARRAY<FLOAT64>>>, e STRUCT<f STRUCT<g INT64>>>",
            Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(
        complexStruct.type,
        Type.struct(
            ImmutableList.of(
                StructField.of("a", Type.bool()),
                StructField.of(
                    "b",
                    Type.array(
                        Type.struct(
                            ImmutableList.of(
                                StructField.of("c", Type.string()),
                                StructField.of("d", Type.array(Type.float64())))))),
                StructField.of(
                    "e",
                    Type.struct(
                        ImmutableList.of(
                            StructField.of(
                                "f",
                                Type.struct(
                                    ImmutableList.of(StructField.of("g", Type.int64()))))))))));
    assertEquals(
        SizedType.typeString(complexStruct.type, null),
        "STRUCT<a BOOL, b ARRAY<STRUCT<c STRING(MAX), d ARRAY<FLOAT64>>>, e STRUCT<f STRUCT<g INT64>>>");
  }

  @Test
  public void testEmbeddingVector() {
    SizedType embeddingVector =
        SizedType.parseSpannerType(
            "ARRAY<FLOAT64>(vector_length=>128)", Dialect.GOOGLE_STANDARD_SQL);

    assertEquals(embeddingVector.type, Type.array(Type.float64()));
    assertEquals(embeddingVector.arrayLength, Integer.valueOf(128));
    assertEquals(
        SizedType.typeString(embeddingVector.type, null, 128, false),
        "ARRAY<FLOAT64>(vector_length=>128)");
  }

  @Test
  public void testPgEmbeddingVector() {
    SizedType embeddingVectorPg =
        SizedType.parseSpannerType("double precision[] vector length 4", Dialect.POSTGRESQL);

    assertEquals(embeddingVectorPg.type, Type.pgArray(Type.pgFloat8()));
    assertEquals(embeddingVectorPg.arrayLength, Integer.valueOf(4));
    assertEquals(
        SizedType.typeString(embeddingVectorPg.type, null, 4, false),
        "double precision[] vector length 4");
  }

  @Test
  public void testProtoEnum() {
    SizedType proto = SizedType.parseSpannerType("PROTO<a.b.proto>", Dialect.GOOGLE_STANDARD_SQL);
    SizedType protoArray =
        SizedType.parseSpannerType("ARRAY<PROTO<a.b.proto>>", Dialect.GOOGLE_STANDARD_SQL);
    SizedType enumType = SizedType.parseSpannerType("ENUM<a.b.enum>", Dialect.GOOGLE_STANDARD_SQL);
    SizedType enumArray =
        SizedType.parseSpannerType("ARRAY<ENUM<a.b.enum>>", Dialect.GOOGLE_STANDARD_SQL);

    assertEquals(proto.type, Type.proto("a.b.proto"));
    assertEquals(
        SizedType.typeString(proto.type, null, /* outputAsDdlRepresentation= */ true), "a.b.proto");

    assertEquals(protoArray.type, Type.array(Type.proto("a.b.proto")));
    assertEquals(
        SizedType.typeString(protoArray.type, null, /* outputAsDdlRepresentation= */ true),
        "ARRAY<a.b.proto>");
    assertEquals(enumType.type, Type.protoEnum("a.b.enum"));
    assertEquals(
        SizedType.typeString(enumType.type, null, /* outputAsDdlRepresentation= */ true),
        "a.b.enum");
    assertEquals(enumArray.type, Type.array(Type.protoEnum("a.b.enum")));
    assertEquals(
        SizedType.typeString(enumArray.type, null, /* outputAsDdlRepresentation= */ true),
        "ARRAY<a.b.enum>");
  }
}
