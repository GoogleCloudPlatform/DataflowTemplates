package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GCSSpannerDVTest {

  @Test
  public void testCreateSpannerConfig() {
    String[] args = new String[] {"--projectId=test-project", "--instanceId=test-instance", "--databaseId=test-database"};
    GCSSpannerDVOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSSpannerDVOptions.class);
    assertNotNull(GCSSpannerDV.createSpannerConfig(options));
  }
}
