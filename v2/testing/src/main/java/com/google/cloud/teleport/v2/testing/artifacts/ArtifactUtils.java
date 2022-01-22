package com.google.cloud.teleport.v2.testing.artifacts;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/** Utilities for working with test artifacts. */
public final class ArtifactUtils {
  private ArtifactUtils() {}

  public static String createTestDirName() {
    return String.format("%s-%s",
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC")).format(Instant.now()),
        UUID.randomUUID());
  }

  public static String createTestDirPath(String bucket, String suiteDir, String testDir) {
    return String.format("gs://%s/%s/%s/%s", bucket, suiteDir, testDir, createTestDirName());
  }
}
