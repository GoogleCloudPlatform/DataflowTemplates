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
import java.math.BigDecimal;

/**
 * Visitor interface for Spanner {@link Value}s.
 *
 * <p>This interface implements the Visitor pattern to decouple the operations performed on Spanner
 * values (such as hashing or string formatting) from the underlying data structure. This approach
 * eliminates the need for repetitive {@code switch} statements or type checks across the codebase.
 */
public interface IUnifiedVisitor {
  void visitString(String s);

  void visitInt64(long l);

  void visitFloat64(double d);

  void visitBool(boolean b);

  void visitBytes(byte[] b);

  void visitDate(Date d);

  void visitNumeric(BigDecimal n);

  void visitTimestamp(Timestamp t);

  void visitJson(String j);

  void visitNull();

  void visitDefault(Value v);

  static void dispatch(Value value, IUnifiedVisitor visitor) {
    if (value.isNull()) {
      visitor.visitNull();
      return;
    }

    switch (value.getType().getCode()) {
      case STRING -> visitor.visitString(value.getString());
      case INT64 -> visitor.visitInt64(value.getInt64());
      case FLOAT64 -> visitor.visitFloat64(value.getFloat64());
      case BOOL -> visitor.visitBool(value.getBool());
      case BYTES -> visitor.visitBytes(value.getBytes().toByteArray());
      case DATE -> visitor.visitDate(value.getDate());
      case NUMERIC -> visitor.visitNumeric(value.getNumeric());
      case TIMESTAMP -> visitor.visitTimestamp(value.getTimestamp());
      case JSON -> visitor.visitJson(value.getJson());
      default -> visitor.visitDefault(value);
    }
  }

  static String formatDate(Date date) {
    return String.format("%04d-%02d-%02d", date.getYear(), date.getMonth(), date.getDayOfMonth());
  }
}
