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
  public void testUdfParameterParseMissingDefaultKeyword() {
    assertThrows(
        IllegalArgumentException.class,
        () -> UdfParameter.parse("p1 int32 1", "s1.foo", Dialect.GOOGLE_STANDARD_SQL));
  }
}
