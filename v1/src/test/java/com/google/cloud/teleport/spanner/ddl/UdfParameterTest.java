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
package com.google.cloud.teleport.spanner.ddl;

import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import org.junit.Test;

/** Unit tests for UdfParameter class. */
public class UdfParameterTest {

  @Test
  public void testBasicUdfParameter() {
    UdfParameter udfParameter =
        UdfParameter.builder().functionSpecificName("s1.foo").name("p1").type("string").autoBuild();

    assertThat(udfParameter.toString(), equalToCompressingWhiteSpace("`p1` string"));
  }

  @Test
  public void testBasicPgUdfParameter() {
    UdfParameter udfParameter =
        UdfParameter.builder()
            .functionSpecificName("s1.foo")
            .name("p1")
            .type("TEXT")
            .dialect(Dialect.POSTGRESQL)
            .autoBuild();

    assertThat(udfParameter.toString(), equalToCompressingWhiteSpace("\"p1\" TEXT"));
  }

  @Test
  public void testUdfParameterWithAllOptions() {
    UdfParameter udfParameter =
        UdfParameter.builder()
            .functionSpecificName("s1.foo")
            .name("p1")
            .type("int32")
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .defaultExpression("1")
            .autoBuild();

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("`p1` int32 DEFAULT 1"));
  }

  @Test
  public void testUdfParameterParse() {
    UdfParameter udfParameter =
        UdfParameter.parse("p1 int32 default 1", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("`p1` int32 DEFAULT 1"));
  }

  @Test
  public void testUdfParameterParseMissingPart() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UdfParameter.parse("p1", "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testUdfParameterParseMissingDefaultBody() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UdfParameter.parse("p1 int32 default", "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testUdfParameterParseQuoted() {
    UdfParameter udfParameter =
        UdfParameter.parse("`p 1` int32", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("`p 1` int32"));
    assertEquals("p 1", udfParameter.name());
  }

  @Test
  public void testUdfParameterParsePgQuoted() {
    UdfParameter udfParameter = UdfParameter.parse("\"p 1\" TEXT", "s1.foo", Dialect.POSTGRESQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("\"p 1\" TEXT"));
    assertEquals("p 1", udfParameter.name());
  }

  @Test
  public void testUdfParameterParseQuotedWithDefault() {
    UdfParameter udfParameter =
        UdfParameter.parse("`p1` int32 DEFAULT 5", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("`p1` int32 DEFAULT 5"));
  }

  @Test
  public void testUdfParameterParsePgTypeWithSpace() {
    UdfParameter udfParameter =
        UdfParameter.parse("p1 double precision", "s1.foo", Dialect.POSTGRESQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("\"p1\" double precision"));
    assertThat(udfParameter.type(), equalToCompressingWhiteSpace("double precision"));
  }

  @Test
  public void testUdfParameterParsePgMultiWordTypeWithDefaultKeywordInLiteral() {
    // As reported by INFORMATION_SCHEMA.PARAMETERS for `"Str" text DEFAULT 'it''s the DEFAULT
    // value'`.
    UdfParameter udfParameter =
        UdfParameter.parse(
            "\"Str\" character varying DEFAULT 'it''s the DEFAULT value'::text",
            "s1.foo",
            Dialect.POSTGRESQL);
    assertEquals("Str", udfParameter.name());
    assertEquals("character varying", udfParameter.type());
    assertEquals("'it''s the DEFAULT value'::text", udfParameter.defaultExpression());
  }

  @Test
  public void testUdfParameterParseGsqlStructTypes() {
    // INFORMATION_SCHEMA.PARAMETERS.DATA_TYPE reports struct types with spaces.
    UdfParameter struct =
        UdfParameter.parse("`p1` STRUCT<x INT64, y STRING>", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("p1", struct.name());
    assertEquals("STRUCT<x INT64, y STRING>", struct.type());
    assertEquals(null, struct.defaultExpression());

    UdfParameter arrayOfStruct =
        UdfParameter.parse(
            "`p1` ARRAY<STRUCT<x INT64, y STRING>> DEFAULT [STRUCT(1, 'a')]",
            "s1.foo",
            Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("p1", arrayOfStruct.name());
    assertEquals("ARRAY<STRUCT<x INT64, y STRING>>", arrayOfStruct.type());
    assertEquals("[STRUCT(1, 'a')]", arrayOfStruct.defaultExpression());
    assertEquals(
        arrayOfStruct,
        UdfParameter.parse(arrayOfStruct.prettyPrint(), "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testUdfParameterParseCaseInsensitiveDefault() {
    UdfParameter udfParameter =
        UdfParameter.parse("p1 int32 dEfAuLt 5", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);

    assertThat(udfParameter.prettyPrint(), equalToCompressingWhiteSpace("`p1` int32 DEFAULT 5"));
    assertThat(udfParameter.defaultExpression(), equalToCompressingWhiteSpace("5"));
  }

  @Test
  public void testUdfParameterParseDefaultExpressionWithDefault() {
    // Tests when the default expression itself contains the word 'default'
    UdfParameter udfParameter =
        UdfParameter.parse(
            "p1 string DEFAULT 'default string'", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);

    assertThat(
        udfParameter.prettyPrint(),
        equalToCompressingWhiteSpace("`p1` string DEFAULT 'default string'"));
    assertThat(udfParameter.defaultExpression(), equalToCompressingWhiteSpace("'default string'"));
  }

  @Test
  public void testUdfParameterParseEmptyString() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UdfParameter.parse("", "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testUdfParameterParseNullString() {
    assertThrows(
        NullPointerException.class,
        () -> UdfParameter.parse(null, "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testUdfParameterParseLeadingAndTrailingWhitespace() {
    UdfParameter udfParameter =
        UdfParameter.parse("  \"pQ\" integer DEFAULT 1  ", "s1.foo", Dialect.POSTGRESQL);

    assertThat(
        udfParameter.prettyPrint(), equalToCompressingWhiteSpace("\"pQ\" integer DEFAULT 1"));
    assertEquals("pQ", udfParameter.name());
    assertThat(udfParameter.defaultExpression(), equalToCompressingWhiteSpace("1"));
  }

  @Test
  public void testUdfParameterParsePgMissingDefaultExpression() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UdfParameter.parse("p1 text DEFAULT", "s1.foo", Dialect.POSTGRESQL));
  }

  @Test
  public void testUdfParameterParseMultiLineDefault() {
    UdfParameter gsql =
        UdfParameter.parse("`p1` INT64 DEFAULT 1\n+ 2", "s1.foo", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("p1", gsql.name());
    assertEquals("INT64", gsql.type());
    assertEquals("1\n+ 2", gsql.defaultExpression());

    UdfParameter pg =
        UdfParameter.parse("\"p1\" bigint DEFAULT (1\n+ 2)", "s1.foo", Dialect.POSTGRESQL);
    assertEquals("p1", pg.name());
    assertEquals("bigint", pg.type());
    assertEquals("(1\n+ 2)", pg.defaultExpression());
  }

  @Test
  public void testUdfParameterParseRoundTrip() {
    // Parameters scanned from the information schema carry unquoted names; parsing their
    // pretty-printed (quoted) form, as done on import, must produce an equal parameter.
    for (Dialect dialect : Dialect.values()) {
      UdfParameter scanned =
          UdfParameter.builder(dialect)
              .functionSpecificName("s1.foo")
              .name("pQ")
              .type(dialect == Dialect.POSTGRESQL ? "bigint" : "INT64")
              .defaultExpression("1")
              .autoBuild();
      assertEquals(scanned, UdfParameter.parse(scanned.prettyPrint(), "s1.foo", dialect));
    }
  }
}
