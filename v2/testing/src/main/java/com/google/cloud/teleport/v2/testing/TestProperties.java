package com.google.cloud.teleport.v2.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for accessing system properties set for the test.
 *
 * <p>The values should be passed to the test like `-Dkey=value`. For instance,
 * `-Dproject=my-project`.
 */
public final class TestProperties {
  private static final Logger LOG = LoggerFactory.getLogger(TestProperties.class);

  public static final String ACCESS_TOKEN_KEY = "accessToken";
  public static final String ARTIFACT_BUCKET_KEY = "artifactBucket";
  public static final String PROJECT_KEY = "project";
  public static final String SPEC_PATH_KEY = "specPath";

  private static String accessToken;
  private static String artifactBucket;
  private static String project;
  private static String specPath;

  private final Map<String, Boolean> initialized;

  public TestProperties() {
    initialized = new HashMap<>();
    initialized.put(ACCESS_TOKEN_KEY, false);
    initialized.put(ARTIFACT_BUCKET_KEY, false);
    initialized.put(PROJECT_KEY, false);
    initialized.put(SPEC_PATH_KEY, false);
  }

  public String accessToken() {
    if (!initialized.get(ACCESS_TOKEN_KEY)) {
      accessToken = System.getProperty(ACCESS_TOKEN_KEY, null);
      initialized.replace(ACCESS_TOKEN_KEY, true);
    }
    return accessToken;
  }

  public String artifactBucket() {
    if (!initialized.get(ARTIFACT_BUCKET_KEY)) {
      artifactBucket = System.getProperty(ARTIFACT_BUCKET_KEY, null);
      checkState(artifactBucket != null, "%s is required", ARTIFACT_BUCKET_KEY);
      initialized.replace(ARTIFACT_BUCKET_KEY, true);
    }
    return artifactBucket;
  }

  public String project() {
    if (!initialized.get(PROJECT_KEY)) {
      project = System.getProperty(PROJECT_KEY, null);
      checkState(project != null, "%s is required", PROJECT_KEY);
      initialized.replace(PROJECT_KEY, true);
    }
    return project;
  }

  public String specPath() {
    if (!initialized.get(SPEC_PATH_KEY)) {
      specPath = System.getProperty(SPEC_PATH_KEY, null);
      checkState(specPath != null, "%s is required", SPEC_PATH_KEY);
      initialized.replace(SPEC_PATH_KEY, true);
    }
    return specPath;
  }
}
