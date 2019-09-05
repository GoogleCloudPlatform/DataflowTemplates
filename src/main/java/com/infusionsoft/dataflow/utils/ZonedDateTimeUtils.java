package com.infusionsoft.dataflow.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ZonedDateTimeUtils {

  private static final DateTimeFormatter FLAGSHIP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  public static final ZoneId UTC = ZoneId.of("UTC");

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .withZone(UTC);

  public static ZonedDateTime parseFlagshipDate(String str) {
    return ZonedDateTime.parse(str, FLAGSHIP);
  }

  public static String formatForGql(ZonedDateTime timestamp) {
    checkNotNull(timestamp, "timestamp must not be null");

    return String.format("DATETIME('%s')", FORMATTER.format(timestamp.withZoneSameInstant(UTC)));
  }
}
