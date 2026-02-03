package com.google.cloud.teleport.v2.dto;

import com.google.cloud.spanner.Value;

/**
 * Visitor interface for Spanner {@link Value}s.
 */
public interface SpannerValueVisitor {
  void visitString(String s);
  void visitInt64(long l);
  void visitFloat64(double d);
  void visitBool(boolean b);
  void visitBytes(byte[] b);
  void visitDate(com.google.cloud.Date d);
  void visitNumeric(java.math.BigDecimal n);
  void visitTimestamp(com.google.cloud.Timestamp t);
  void visitJson(String j);
  void visitNull();
  void visitDefault(Value v);

  static void dispatch(Value value, SpannerValueVisitor visitor) {
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

  static String formatDate(com.google.cloud.Date date) {
    return String.format("%04d-%02d-%02d",
        date.getYear(), date.getMonth(), date.getDayOfMonth());
  }
}
