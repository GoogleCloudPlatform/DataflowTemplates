package com.google.cloud.teleport.v2.dto;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import org.apache.commons.codec.binary.Base64;

/**
 * Visitor that converts Spanner values to their String representation.
 */
public class SpannerStringVisitor implements SpannerValueVisitor {
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
  public void visitInt64(long l) {
    result = String.valueOf(l);
  }

  @Override
  public void visitFloat64(double d) {
    result = String.valueOf(d);
  }

  @Override
  public void visitBool(boolean b) {
    result = String.valueOf(b);
  }

  @Override
  public void visitBytes(byte[] b) {
    result = Base64.encodeBase64String(b);
  }

  @Override
  public void visitDate(Date d) {
    result = SpannerValueVisitor.formatDate(d);
  }

  @Override
  public void visitNumeric(BigDecimal n) {
    result = String.valueOf(n);
  }

  @Override
  public void visitTimestamp(Timestamp t) {
    result = t.toString();
  }

  @Override
  public void visitJson(String j) {
    result = j;
  }

  @Override
  public void visitDefault(Value v) {
    result = v.toString();
  }
}
