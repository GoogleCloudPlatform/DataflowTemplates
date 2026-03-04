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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IUnifiedVisitorTest {

  @Test
  public void testDispatchMatchesString() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    String input = "test-string";
    Value value = Value.string(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitString(input);
  }

  @Test
  public void testDispatchMatchesInt64() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    long input = 123456789L;
    Value value = Value.int64(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitInt64(input);
  }

  @Test
  public void testDispatchMatchesFloat64() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    double input = 123.456;
    Value value = Value.float64(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitFloat64(input);
  }

  @Test
  public void testDispatchMatchesBool() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    boolean input = true;
    Value value = Value.bool(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitBool(input);
  }

  @Test
  public void testDispatchMatchesBytes() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    byte[] input = "test-bytes".getBytes();
    Value value = Value.bytes(com.google.cloud.ByteArray.copyFrom(input));

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitBytes(input);
  }

  @Test
  public void testDispatchMatchesDate() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    Date input = Date.parseDate("2024-02-06");
    Value value = Value.date(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitDate(input);
  }

  @Test
  public void testDispatchMatchesNumeric() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    BigDecimal input = new BigDecimal("123.456");
    Value value = Value.numeric(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitNumeric(input);
  }

  @Test
  public void testDispatchMatchesTimestamp() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    Timestamp input = Timestamp.parseTimestamp("2024-02-06T12:00:00Z");
    Value value = Value.timestamp(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitTimestamp(input);
  }

  @Test
  public void testDispatchMatchesJson() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    String input = "{\"key\": \"value\"}";
    Value value = Value.json(input);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitJson(input);
  }

  @Test
  public void testDispatchMatchesNull() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    Value value = Value.string(null);

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitNull();
  }

  @Test
  public void testDispatchMatchesDefault() {
    IUnifiedVisitor visitor = mock(IUnifiedVisitor.class);
    // STRUCT is not explicitly handled in the switch case, so it should go to
    // default
    Value value = Value.struct(com.google.cloud.spanner.Struct.newBuilder().build());

    IUnifiedVisitor.dispatch(value, visitor);

    verify(visitor).visitDefault(value);
  }

  @Test
  public void testFormatDate() {
    Date date = Date.fromYearMonthDay(2024, 2, 6);
    String formatted = IUnifiedVisitor.formatDate(date);
    assertEquals("2024-02-06", formatted);
  }
}
