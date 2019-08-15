package com.infusionsoft.dataflow.utils;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ZonedDateTimeUtils {

  private static final DateTimeFormatter FLAGSHIP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  public static ZonedDateTime parseFlagshipDate(String str) {
    return ZonedDateTime.parse(str, FLAGSHIP);
  }
}
