package com.infusionsoft.dataflow.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.protobuf.util.Timestamps;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class JavaTimeUtils {

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

  public static ZonedDateTime toZonedDateTime(com.google.protobuf.Timestamp timestamp) {
    checkNotNull(timestamp, "timestamp must not be null");

    final Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());

    return ZonedDateTime.ofInstant(instant, UTC);
  }

  public static com.google.protobuf.Timestamp toTimestamp(ZonedDateTime timestamp) {
    checkNotNull(timestamp, "timestamp must not be null");

    final Instant instant = timestamp.toInstant();

    return Timestamps.fromMillis(instant.toEpochMilli());
  }
}
