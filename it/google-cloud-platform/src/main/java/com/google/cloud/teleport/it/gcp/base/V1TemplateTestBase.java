package com.google.cloud.teleport.it.gcp.base;

import java.io.IOException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.TemplateTestBase; // Beam IT base class
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for V1 Classic Dataflow Template integration tests, providing enhanced logging for job
 * information.
 */
public abstract class V1TemplateTestBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(V1TemplateTestBase.class);

  // This is the primary method used by TemplateTestBase to launch classic or flex jobs.
  @Override
  public PipelineLauncher.LaunchInfo launchTemplate(
      PipelineLauncher.LaunchConfig.Builder configBuilder, boolean flex) throws IOException {

    PipelineLauncher.LaunchInfo launchInfo = super.launchTemplate(configBuilder, flex);

    // After the job is launched, if it's a V1 classic job (flex == false)
    // and we have valid job info, log the details.
    if (!flex && launchInfo != null && launchInfo.jobId() != null) {
      String projectId = launchInfo.projectId();
      String region = launchInfo.region();
      String jobId = launchInfo.jobId();

      if (projectId != null && region != null) {
        String jobLink =
            String.format(
                "https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s",
                region, jobId, projectId);
        LOG.info("Launched Classic Dataflow Job ID: {}. View Job: {}", jobId, jobLink);
      } else {
        LOG.info(
            "Launched Classic Dataflow Job ID: {} (project/region details missing in LaunchInfo for full link).",
            jobId);
      }
    }
    return launchInfo;
  }
}
