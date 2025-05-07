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
import com.google.cloud.teleport.spanner.ddl.Udf.SqlSecurity;
import org.junit.Test;

/** Unit tests for Udf class. */
public class UdfTest {

  @Test
  public void testBasicUdf() {
    Udf udf = Udf.builder().name("foo").specificName("s1.foo").type("string").build();

    assertThat(
        udf.prettyPrint(), equalToCompressingWhiteSpace("CREATE FUNCTION `foo`() RETURNS string"));

    assertThat(
        udf.toBuilder().build().toString(),
        equalToCompressingWhiteSpace("CREATE FUNCTION `foo`() RETURNS string"));
  }

  @Test
  public void testUdfWithAllOptions() {
    Udf udf =
        Udf.builder()
            .name("foo")
            .specificName("s1.foo")
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .type("string")
            .definition("(SELECT 1)")
            .security(SqlSecurity.INVOKER)
            .addParameter(UdfParameter.parse("p1 int32", "s1.foo", Dialect.GOOGLE_STANDARD_SQL))
            .build();

    assertThat(
        udf.toString(),
        equalToCompressingWhiteSpace(
            "CREATE FUNCTION `foo`(`p1` int32) RETURNS string SQL SECURITY INVOKER AS ((SELECT 1))"));

    assertThat(
        udf.toBuilder().build().toString(),
        equalToCompressingWhiteSpace(
            "CREATE FUNCTION `foo`(`p1` int32) RETURNS string SQL SECURITY INVOKER AS ((SELECT 1))"));
  }

  @Test
  public void testUdfWithInvalidParameter() {
    Udf.Builder udf =
        Udf.builder()
            .name("foo")
            .specificName("s1.foo")
            .type("string")
            .addParameter(UdfParameter.parse("p1 int32", "s1.bar", Dialect.GOOGLE_STANDARD_SQL));

    assertThrows(IllegalArgumentException.class, () -> udf.parameter("p1"));
  }
}
