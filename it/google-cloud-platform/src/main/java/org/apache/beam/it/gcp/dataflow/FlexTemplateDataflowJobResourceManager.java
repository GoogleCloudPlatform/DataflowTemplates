/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.it.gcp.dataflow;

import com.google.auth.Credentials;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlexTemplateDataflowJobResourceManager implements ResourceManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlexTemplateDataflowJobResourceManager.class);
  private Map<String, String> parameters;
  private String templateName;
  private PipelineLauncher.LaunchInfo jobInfo;
  private final LaunchConfig launchConfig;
  private final PipelineLauncher pipelineLauncher;

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static Map<String, String> specPaths = new HashMap<>();

  private FlexTemplateDataflowJobResourceManager(Builder builder) {
    this.parameters = builder.parameters;
    this.templateName = builder.templateName;
    pipelineLauncher = FlexTemplateClient.builder(CREDENTIALS).build();
    synchronized (specPaths) {
      if (!specPaths.containsKey(builder.templateName)) {
        buildAndStageTemplate(builder.templateName, builder.templateModulePath);
      }
    }

    LaunchConfig.Builder launchConfigBuilder =
        LaunchConfig.builder(builder.jobName, specPaths.get(builder.templateName))
            .setParameters(parameters);
    for (Map.Entry<String, Object> entry : builder.environmentVariables.entrySet()) {
      launchConfigBuilder.addEnvironment(entry.getKey(), entry.getValue());
    }
    this.launchConfig = launchConfigBuilder.build();
  }

  public static FlexTemplateDataflowJobResourceManager.Builder builder(String jobName) {
    return new FlexTemplateDataflowJobResourceManager.Builder(jobName);
  }

  /**
   * Launches the dataflow job corresponding to the templateName.
   *
   * @return the jobInfo object
   */
  public PipelineLauncher.LaunchInfo launchJob() throws IOException {
    LaunchInfo launchInfo = pipelineLauncher.launch(PROJECT, REGION, launchConfig);
    LOG.info("Dataflow job started");
    this.jobInfo = launchInfo;

    // if the launch succeeded, setup a thread to cancel job
    if (launchInfo.jobId() != null && !launchInfo.jobId().isEmpty()) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(new TemplateTestBase.CancelJobShutdownHook(pipelineLauncher, launchInfo)));
    }

    return launchInfo;
  }

  public void cleanupAll() {
    if (pipelineLauncher != null) {
      try {
        pipelineLauncher.cleanupAll();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public LaunchInfo getJobInfo() {
    return jobInfo;
  }

  public String getTemplateName() {
    return templateName;
  }

  /** Builder for the {@link FlexTemplateDataflowJobResourceManager}. */
  public static final class Builder {
    private final String jobName;
    private final Map<String, String> parameters;
    private final Map<String, Object> environmentVariables;
    private String templateName;
    private String templateModulePath;

    private Builder(String jobName) {
      this.jobName = jobName;
      this.parameters = new HashMap<>();
      this.environmentVariables = new HashMap<>();
    }

    public String getJobName() {
      return jobName;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public FlexTemplateDataflowJobResourceManager.Builder addParameter(String key, String value) {
      parameters.put(key, value);
      return this;
    }

    public FlexTemplateDataflowJobResourceManager.Builder addEnvironmentVariable(
        String key, Object value) {
      environmentVariables.put(key, value);
      return this;
    }

    public FlexTemplateDataflowJobResourceManager.Builder withTemplateName(String templateName) {
      this.templateName = templateName;
      return this;
    }

    public FlexTemplateDataflowJobResourceManager.Builder withTemplateModulePath(
        String templateModulePath) {
      this.templateModulePath = templateModulePath;
      return this;
    }

    public FlexTemplateDataflowJobResourceManager build() {
      if (templateName == null) {
        throw new IllegalArgumentException(
            "templateName cannot be null in FlexTemplateDataflowJobResourceManager.Builder");
      }
      if (templateModulePath == null) {
        throw new IllegalArgumentException(
            "templateModulePath cannot be null in FlexTemplateDataflowJobResourceManager.Builder");
      }
      return new FlexTemplateDataflowJobResourceManager(this);
    }
  }

  private void buildAndStageTemplate(String templateName, String modulePath) {
    LOG.info("Building and Staging {} template", templateName);

    String prefix = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + "_IT";

    // Use bucketName unless only artifactBucket is provided
    String bucketName;
    if (TestProperties.hasStageBucket()) {
      bucketName = TestProperties.stageBucket();
    } else if (TestProperties.hasArtifactBucket()) {
      bucketName = TestProperties.artifactBucket();
      LOG.warn(
          "-DstageBucket was not specified, using -DartifactBucket ({}) for stage step",
          bucketName);
    } else {
      throw new IllegalArgumentException(
          "-DstageBucket was not specified, so Template can not be staged. Either give a"
              + " -DspecPath or provide a proper -DstageBucket for automatic staging.");
    }

    String[] mavenCmd = buildMavenStageCommand(prefix, bucketName, templateName, modulePath);
    LOG.info("Running command to stage templates: {}", String.join(" ", mavenCmd));

    try {
      Process exec = Runtime.getRuntime().exec(mavenCmd);
      IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
      IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

      if (exec.waitFor() != 0) {
        throw new RuntimeException("Error staging template, check Maven logs.");
      }

      specPaths.put(
          templateName,
          String.format("gs://%s/%s/%s%s", bucketName, prefix, "flex/", templateName));
    } catch (Exception e) {
      throw new IllegalArgumentException("Error staging template", e);
    }
  }

  String[] buildMavenStageCommand(
      String prefix, String bucketName, String templateName, String modulePath) {
    File pom = new File("pom.xml").getAbsoluteFile();
    if (!pom.exists()) {
      throw new IllegalArgumentException(
          "To use FlexTemplateDataflowJobResourceManager, please run in the outermost directory (outside v1 or v2).");
    }
    String pomPath = pom.getAbsolutePath();
    String moduleBuild = "metadata,v2/common," + modulePath;
    pomPath = pomPath.replaceAll("/v2/.*", "/pom.xml");

    return new String[] {
      "mvn",
      "compile",
      "package",
      "-q",
      "-f",
      pomPath,
      "-pl",
      moduleBuild,
      "-am",
      "-PtemplatesStage,pluginOutputDir,splunkDeps,missing-artifact-repos",
      // Skip shading for now due to flakiness / slowness in the process.
      "-DskipShade=" + true,
      "-DskipTests",
      "-Dmaven.test.skip",
      "-Dcheckstyle.skip",
      "-Dmdep.analyze.skip",
      "-Dspotless.check.skip",
      "-Denforcer.skip",
      "-DprojectId=" + TestProperties.project(),
      "-Dregion=" + TestProperties.region(),
      "-DbucketName=" + bucketName,
      "-DgcpTempLocation=" + bucketName,
      "-DstagePrefix=" + prefix,
      "-DtemplateName=" + templateName,
      "-DunifiedWorker=" + System.getProperty("unifiedWorker"),
      // Print stacktrace when command fails
      "-e",
      "-DpluginRunId=" + RandomStringUtils.randomAlphanumeric(16),
    };
  }
}
