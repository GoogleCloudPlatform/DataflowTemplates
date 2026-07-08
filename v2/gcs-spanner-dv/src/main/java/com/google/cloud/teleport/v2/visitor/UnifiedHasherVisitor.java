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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.common.hash.Hasher;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/** Visitor that hashes Spanner values using a provided {@link Hasher}. */
public class UnifiedHasherVisitor implements IUnifiedVisitor {
  private final Hasher hasher;

  public UnifiedHasherVisitor(Hasher hasher) {
    this.hasher = hasher;
  }

  @Override
  public void visitNull() {
    // Null values are encoded with a sentinel byte 0
    hasher.putByte((byte) 0);
  }

  private void markNonNull() {
    // Non null values are encoded with a sentinel byte 1
    hasher.putByte((byte) 1);
  }

  @Override
  public void visitString(String s) {
    // String values are encoded with a sentinel byte 1 followed by the length of the string and the
    // string itself
    // This is done to avoid collisions with empty strings, null values and string concatenation
    // across columns.
    markNonNull();
    hasher.putInt(s.length());
    hasher.putString(s, StandardCharsets.UTF_8);
  }

  @Override
  public void visitInt64(long l) {
    // Int64 values are encoded with a sentinel byte 1 followed by the long value
    markNonNull();
    hasher.putLong(l);
  }

  @Override
  public void visitFloat64(double d) {
    // Float64 values are encoded with a sentinel byte 1 followed by the double value
    markNonNull();
    hasher.putDouble(d);
  }

  @Override
  public void visitBool(boolean b) {
    // Bool values are encoded with a sentinel byte 1 followed by the boolean value
    markNonNull();
    hasher.putBoolean(b);
  }

  @Override
  public void visitBytes(byte[] b) {
    // Bytes values are encoded with a sentinel byte 1 followed by the byte array
    markNonNull();
    hasher.putBytes(b);
  }

  @Override
  public void visitDate(Date d) {
    // Date values are encoded with a sentinel byte 1 followed by the date in yyyy-MM-dd format
    markNonNull();
    hasher.putString(IUnifiedVisitor.formatDate(d), StandardCharsets.UTF_8);
  }

  @Override
  public void visitNumeric(BigDecimal n) {
    // Numeric values are encoded with a sentinel byte 1 followed by the numeric value as a string
    markNonNull();
    hasher.putString(n.toString(), StandardCharsets.UTF_8);
  }

  @Override
  public void visitTimestamp(Timestamp t) {
    // Timestamp values are encoded with a sentinel byte 1 followed by the timestamp as a string
    markNonNull();
    hasher.putString(t.toString(), StandardCharsets.UTF_8);
  }

  @Override
  public void visitJson(String j) {
    // Json values are encoded with a sentinel byte 1 followed by the json value as a string
    markNonNull();
    hasher.putString(j, StandardCharsets.UTF_8);
  }

  @Override
  public void visitDefault(Value v) {
    // Values which are not supported are encoded with a sentinel byte 1 followed by the value as a
    // string
    // TODO: Determine if this is the right approach if we should throw an exception here.
    markNonNull();
    hasher.putString(v.toString(), StandardCharsets.UTF_8);
  }
}
