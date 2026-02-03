package com.google.cloud.teleport.v2.dto;

import com.google.common.hash.Hasher;
import java.nio.charset.StandardCharsets;

/**
 * Visitor that hashes Avro values using a provided {@link Hasher}.
 * mimics {@link SpannerHasherVisitor} behavior for compatible types.
 */
public class AvroHasherVisitor implements AvroValueVisitor {
  private final Hasher hasher;

  public AvroHasherVisitor(Hasher hasher) {
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
  public void visitInt(int i) {
    markNonNull();
    // Spanner INT64 is Java long. Avro INT is Java int.
    // We promote to long for consistency if we want to match Spanner INT64 hashes for integer values.
    // However, strictly speaking, INT and INT64 are different types.
    // But often Avro int -> Spanner int64 mapping happens.
    // Let's assume we want to hash the *value* consistently.
    hasher.putLong((long) i); 
  }

  @Override
  public void visitLong(long l) {
    markNonNull();
    hasher.putLong(l);
  }

  @Override
  public void visitFloat(float f) {
    markNonNull();
    // Spanner FLOAT64 is Java double. Avro FLOAT is Java float.
    // Promote to double for potential consistency.
    hasher.putDouble((double) f);
  }

  @Override
  public void visitDouble(double d) {
    markNonNull();
    hasher.putDouble(d);
  }

  @Override
  public void visitBoolean(boolean b) {
    markNonNull();
    hasher.putBoolean(b);
  }

  @Override
  public void visitBytes(byte[] b) {
    markNonNull();
    hasher.putBytes(b);
  }

  @Override
  public void visitDefault(Object value) {
    markNonNull();
    // Fallback for complex types (Records, Arrays, Maps, Enums, Fixed)
    // SpannerVisitor uses v.toString() for default.
    // attributes.toString() in Avro usually produces a JSON-like string.
    hasher.putString(value.toString(), StandardCharsets.UTF_8);
  }
}
