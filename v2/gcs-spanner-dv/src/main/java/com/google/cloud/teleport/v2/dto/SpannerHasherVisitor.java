package com.google.cloud.teleport.v2.dto;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.common.hash.Hasher;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Visitor that hashes Spanner values using a provided {@link Hasher}.
 */
public class SpannerHasherVisitor implements SpannerValueVisitor {
  private final Hasher hasher;

  public SpannerHasherVisitor(Hasher hasher) {
    this.hasher = hasher;
  }

  @Override
  public void visitNull() {
    hasher.putByte((byte) 0);
  }

  private void markNonNull() {
    hasher.putByte((byte) 1);
  }

  @Override
  public void visitString(String s) {
    markNonNull();
    hasher.putInt(s.length());
    hasher.putString(s, StandardCharsets.UTF_8);
  }

  @Override
  public void visitInt64(long l) {
    markNonNull();
    hasher.putLong(l);
  }

  @Override
  public void visitFloat64(double d) {
    markNonNull();
    hasher.putDouble(d);
  }

  @Override
  public void visitBool(boolean b) {
    markNonNull();
    hasher.putBoolean(b);
  }

  @Override
  public void visitBytes(byte[] b) {
    markNonNull();
    hasher.putBytes(b);
  }

  @Override
  public void visitDate(Date d) {
    markNonNull();
    hasher.putString(SpannerValueVisitor.formatDate(d), StandardCharsets.UTF_8);
  }

  @Override
  public void visitNumeric(BigDecimal n) {
    markNonNull();
    hasher.putString(n.toString(), StandardCharsets.UTF_8);
  }

  @Override
  public void visitTimestamp(Timestamp t) {
    markNonNull();
    hasher.putString(t.toString(), StandardCharsets.UTF_8);
  }

  @Override
  public void visitJson(String j) {
    markNonNull();
    hasher.putString(j, StandardCharsets.UTF_8);
  }

  @Override
  public void visitDefault(Value v) {
    markNonNull();
    hasher.putString(v.toString(), StandardCharsets.UTF_8);
  }
}
