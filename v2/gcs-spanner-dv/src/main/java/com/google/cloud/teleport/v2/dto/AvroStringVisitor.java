package com.google.cloud.teleport.v2.dto;

import org.apache.commons.codec.binary.Base64;

/**
 * Visitor that converts Avro values to their String representation.
 * Mimics {@link SpannerStringVisitor}.
 */
public class AvroStringVisitor implements AvroValueVisitor {
  private String result;

  public String getResult() {
    return result;
  }

  @Override
  public void visitNull() {
    result = "";
  }

  @Override
  public void visitString(String s) {
    result = s;
  }

  @Override
  public void visitInt(int i) {
    result = String.valueOf(i);
  }

  @Override
  public void visitLong(long l) {
    result = String.valueOf(l);
  }

  @Override
  public void visitFloat(float f) {
    result = String.valueOf(f);
  }

  @Override
  public void visitDouble(double d) {
    result = String.valueOf(d);
  }

  @Override
  public void visitBoolean(boolean b) {
    result = String.valueOf(b);
  }

  @Override
  public void visitBytes(byte[] b) {
    result = Base64.encodeBase64String(b);
  }

  @Override
  public void visitDefault(Object value) {
    result = value.toString();
  }
}
