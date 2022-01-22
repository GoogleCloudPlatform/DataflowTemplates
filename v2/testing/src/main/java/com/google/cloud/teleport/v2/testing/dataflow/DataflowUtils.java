package com.google.cloud.teleport.v2.testing.dataflow;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class DataflowUtils {
  private DataflowUtils() {}

  public static String createJobName(String prefix) {
    return String.format("%s-%s",
        prefix,
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")).format(Instant.now()));
  }
}
