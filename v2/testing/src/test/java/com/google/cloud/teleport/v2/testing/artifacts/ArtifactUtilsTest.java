package com.google.cloud.teleport.v2.testing.artifacts;

import static com.google.cloud.teleport.v2.testing.artifacts.ArtifactUtils.createTestDirName;
import static com.google.cloud.teleport.v2.testing.artifacts.ArtifactUtils.createTestDirPath;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ArtifactUtilsTest {

  // Not matching exact date, since it may fail if the test runs close enough to the change of
  // date.
  private static final String TEST_DIR_REGEX =
      "\\d{8}-[a-fA-F0-9]{8}-([a-fA-F0-9]{4}-){3}[a-fA-F0-9]{12}";

  private static final String BUCKET = "test-bucket";
  private static final String TEST_SUITE_DIR = "artifact-utils-test";

  @Test
  public void testCreateTestDirName() {
    assertThat(createTestDirName()).matches(TEST_DIR_REGEX);
  }

  @Test
  public void testCreateTestDirPath() {
    String name = "test-create-test-dir-path";

    String actual = createTestDirPath(BUCKET, TEST_SUITE_DIR, name);

    String expectedPattern = String.format("gs://%s/%s/%s/%s", BUCKET, TEST_SUITE_DIR, name, TEST_DIR_REGEX);

    assertThat(actual).matches(expectedPattern);
  }
}