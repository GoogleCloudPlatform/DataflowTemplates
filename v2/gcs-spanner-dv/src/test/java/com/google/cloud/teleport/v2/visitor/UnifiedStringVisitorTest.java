/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.visitor;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UnifiedStringVisitorTest {

  @Test
  public void testVisitString() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    String input = "test-string";

    visitor.visitString(input);

    assertEquals(input, visitor.getResult());
  }

  @Test
  public void testVisitInt64() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    long input = 123456789L;

    visitor.visitInt64(input);

    assertEquals(String.valueOf(input), visitor.getResult());
  }

  @Test
  public void testVisitFloat64() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    double input = 123.456;

    visitor.visitFloat64(input);

    assertEquals(String.valueOf(input), visitor.getResult());
  }

  @Test
  public void testVisitBool() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    boolean input = true;

    visitor.visitBool(input);

    assertEquals(String.valueOf(input), visitor.getResult());
  }

  @Test
  public void testVisitBytes() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    byte[] input = "test-bytes".getBytes();

    visitor.visitBytes(input);

    assertEquals(Base64.encodeBase64String(input), visitor.getResult());
  }

  @Test
  public void testVisitDate() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    Date input = Date.parseDate("2024-02-06");

    visitor.visitDate(input);

    assertEquals("2024-02-06", visitor.getResult());
  }

  @Test
  public void testVisitNumeric() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    BigDecimal input = new BigDecimal("123.456");

    visitor.visitNumeric(input);

    assertEquals(String.valueOf(input), visitor.getResult());
  }

  @Test
  public void testVisitTimestamp() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    Timestamp input = Timestamp.parseTimestamp("2024-02-06T12:00:00Z");

    visitor.visitTimestamp(input);

    assertEquals(input.toString(), visitor.getResult());
  }

  @Test
  public void testVisitJson() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    String input = "{\"key\": \"value\"}";

    visitor.visitJson(input);

    assertEquals(input, visitor.getResult());
  }

  @Test
  public void testVisitNull() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();

    visitor.visitNull();

    assertEquals("", visitor.getResult());
  }

  @Test
  public void testVisitDefault() {
    UnifiedStringVisitor visitor = new UnifiedStringVisitor();
    Value input = Value.string("default-value");

    visitor.visitDefault(input);

    assertEquals(input.toString(), visitor.getResult());
  }
}
