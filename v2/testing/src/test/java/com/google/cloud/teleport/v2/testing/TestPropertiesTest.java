package com.google.cloud.teleport.v2.testing;

import static com.google.common.truth.Truth.assertThat;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TestPropertiesTest {
  private static final String ACCESS_TOKEN = "some-token";
  private static final String ARTIFACT_BUCKET = "test-bucket";
  private static final String PROJECT = "test-project";
  private static final String SPEC_PATH = "gs://test-bucket/some/spec/path";

  private final TestProperties properties = new TestProperties();

  @After
  public void tearDown() {
    System.clearProperty(TestProperties.ACCESS_TOKEN_KEY);
    System.clearProperty(TestProperties.ARTIFACT_BUCKET_KEY);
    System.clearProperty(TestProperties.PROJECT_KEY);
    System.clearProperty(TestProperties.SPEC_PATH_KEY);
  }

  @Test
  public void testAllPropertiesSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(properties.accessToken()).isEqualTo(ACCESS_TOKEN);
    assertThat(properties.artifactBucket()).isEqualTo(ARTIFACT_BUCKET);
    assertThat(properties.project()).isEqualTo(PROJECT);
    assertThat(properties.specPath()).isEqualTo(SPEC_PATH);
  }

  @Test
  public void testAccessTokenNotSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(properties.accessToken()).isNull();
    assertThat(properties.artifactBucket()).isEqualTo(ARTIFACT_BUCKET);
    assertThat(properties.project()).isEqualTo(PROJECT);
    assertThat(properties.specPath()).isEqualTo(SPEC_PATH);
  }

  @Test(expected = IllegalStateException.class)
  public void testArtifactBucketNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    properties.artifactBucket();
  }

  @Test(expected = IllegalStateException.class)
  public void testProjectNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    properties.project();
  }

  @Test(expected = IllegalStateException.class)
  public void testSpecPathNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);

    properties.specPath();
  }
}