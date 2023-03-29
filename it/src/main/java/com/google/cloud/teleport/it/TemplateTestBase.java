/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.common.IORedirectUtil;
import com.google.cloud.teleport.it.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.dataflow.DirectRunnerClient;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Config;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateCreationParameters;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.metadata.util.MetadataUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Templates. It wraps around tests that extend it to stage the Templates when
 * <strong>-DspecPath</strong> isn't provided.
 *
 * <p>It is required to use @TemplateIntegrationTest to specify which template is under test.
 */
@RunWith(JUnit4.class)
public abstract class TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateTestBase.class);

  public String testName;

  @Rule
  public TestRule watcher =
      new TestWatcher() {
        protected void starting(Description description) {
          LOG.info(
              "Starting integration test {}.{}",
              description.getClassName(),
              description.getMethodName());
          testName = description.getMethodName();
        }
      };

  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();
  protected static final String HOST_IP = TestProperties.hostIp();

  protected String specPath;
  protected Credentials credentials;
  protected CredentialsProvider credentialsProvider;
  protected String artifactBucketName;
  protected String testId;

  /** Cache to avoid staging the same template multiple times on the same execution. */
  private static final Map<String, String> stagedTemplates = new HashMap<>();

  protected Template template;
  private Class<?> templateClass;

  /** Client to interact with GCS. */
  protected GcsArtifactClient gcsClient;

  /**
   * Client to interact with GCS.
   *
   * @deprecated Will be removed in favor of {@link #gcsClient}.
   */
  @Deprecated protected GcsArtifactClient artifactClient;

  @Before
  public void setUpBase() {

    testId = PipelineUtils.createJobName("test");

    TemplateIntegrationTest annotation = null;
    try {
      Method testMethod = getClass().getMethod(testName);
      annotation = testMethod.getAnnotation(TemplateIntegrationTest.class);
    } catch (NoSuchMethodException e) {
      // ignore error
    }
    if (annotation == null) {
      annotation = getClass().getAnnotation(TemplateIntegrationTest.class);
    }
    if (annotation == null) {
      LOG.warn(
          "{} did not specify which template is tested using @TemplateIntegrationTest, skipping.",
          getClass());
      return;
    }
    templateClass = annotation.value();
    template = getTemplateAnnotation(annotation, templateClass);
    if (template == null) {
      return;
    }
    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = TestProperties.buildCredentialsFromEnv();
    }

    // Prefer artifactBucket, but use the staging one if none given
    if (TestProperties.hasArtifactBucket()) {
      artifactBucketName = TestProperties.artifactBucket();
    } else if (TestProperties.hasStageBucket()) {
      artifactBucketName = TestProperties.stageBucket();
    }
    if (artifactBucketName != null) {
      Storage storageClient = createStorageClient(credentials);
      gcsClient =
          GcsArtifactClient.builder(storageClient, artifactBucketName, getClass().getSimpleName())
              .build();

      // Keep name compatibility, for now
      artifactClient = gcsClient;

    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. Storage Client will not be"
              + " created automatically.");
    }

    credentialsProvider = FixedCredentialsProvider.create(credentials);

    if (TestProperties.specPath() != null && !TestProperties.specPath().isEmpty()) {
      LOG.info("A spec path was given, not staging template {}", template.name());
      specPath = TestProperties.specPath();
    } else if (stagedTemplates.containsKey(template.name())) {
      specPath = stagedTemplates.get(template.name());
    } else if (System.getProperty("directRunnerTest") == null) {
      LOG.info("Preparing test for {} ({})", template.name(), templateClass);

      String prefix = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()) + "_IT";

      File pom = new File("pom.xml").getAbsoluteFile();
      if (!pom.exists()) {
        throw new IllegalArgumentException(
            "To use tests staging templates, please run in the Maven module directory containing"
                + " the template.");
      }

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

      String[] mavenCmd = buildMavenStageCommand(prefix, pom, bucketName);
      LOG.info("Running command to stage templates: {}", String.join(" ", mavenCmd));

      try {
        Process exec = Runtime.getRuntime().exec(mavenCmd);
        IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
        IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

        if (exec.waitFor() != 0) {
          throw new RuntimeException("Error staging template, check Maven logs.");
        }

        boolean flex =
            template.flexContainerName() != null && !template.flexContainerName().isEmpty();
        specPath =
            String.format(
                "gs://%s/%s/%s%s", bucketName, prefix, flex ? "flex/" : "", template.name());
        LOG.info("Template staged successfully! Path: {}", specPath);

        stagedTemplates.put(template.name(), specPath);

      } catch (Exception e) {
        throw new IllegalArgumentException("Error staging template", e);
      }
    }
  }

  private Template getTemplateAnnotation(
      TemplateIntegrationTest annotation, Class<?> templateClass) {
    String templateName = annotation.template();
    Template[] templateAnnotations = templateClass.getAnnotationsByType(Template.class);
    if (templateAnnotations.length == 0) {
      throw new RuntimeException(
          String.format(
              "Template mentioned in @TemplateIntegrationTest for %s does not contain a @Template"
                  + " annotation.",
              getClass()));
    } else if (templateAnnotations.length == 1) {
      return templateAnnotations[0];
    } else if (templateName.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "Template mentioned in @TemplateIntegrationTest for %s contains multiple @Template"
                  + " annotations. Please provide templateName field in @TemplateIntegrationTest.",
              getClass()));
    }
    for (Template template : templateAnnotations) {
      if (template.name().equals(templateName)) {
        return template;
      }
    }
    throw new RuntimeException(
        "templateName does not match any Template annotations. Please recheck"
            + " @TemplateIntegrationTest.");
  }

  /**
   * Create the Maven command line used to stage the specific template using the Templates Plugin.
   * It identifies whether the template is v1 (Classic) or v2 (Flex) to setup the Maven reactor
   * accordingly.
   */
  private String[] buildMavenStageCommand(String prefix, File pom, String bucketName) {
    String pomPath = pom.getAbsolutePath();
    String moduleBuild;

    // Classic templates run on parent pom and -pl v1
    if (pomPath.endsWith("v1/pom.xml")) {
      pomPath = new File(pom.getParentFile().getParentFile(), "pom.xml").getAbsolutePath();
      moduleBuild = String.join(",", List.of("metadata", "it", "v1"));
    } else if (pomPath.contains("v2/")) {
      // Flex templates run on parent pom and -pl {path-to-folder}
      moduleBuild = String.join(",", getModulesBuild(pomPath));
      pomPath = pomPath.replaceAll("/v2/.*", "/pom.xml");
    } else {
      LOG.warn(
          "Specific module POM was not found, so scanning all modules... Stage step may take a"
              + " little longer.");
      moduleBuild = ".";
    }

    return new String[] {
      "mvn",
      "compile",
      "package",
      "-q",
      "-f",
      pomPath,
      "-pl",
      moduleBuild,
      // Do not make all dependencies every time. Faster but requires prior `mvn install`.
      //      "-am",
      "-PtemplatesStage,pluginOutputDir",
      "-DpluginRunId=" + RandomStringUtils.randomAlphanumeric(16),
      // Skip shading for now due to flakiness / slowness in the process.
      "-DskipShade",
      "-DskipTests",
      "-Dcheckstyle.skip",
      "-Dmdep.analyze.skip",
      "-Dspotless.check.skip",
      "-Denforcer.skip",
      "-DprojectId=" + TestProperties.project(),
      "-Dregion=" + TestProperties.region(),
      "-DbucketName=" + bucketName,
      "-DstagePrefix=" + prefix,
      "-DtemplateName=" + template.name(),
      // Print stacktrace when command fails
      "-e"
    };
  }

  private List<String> getModulesBuild(String pomPath) {
    List<String> modules = new ArrayList<>();
    modules.add("metadata");
    modules.add("it");
    modules.add("v2/common");

    // Force building specific common areas. This is much faster than using -am when staging.
    if (pomPath.contains("kafka")) {
      modules.add("v2/kafka-common");
    }
    if (pomPath.contains("elasticsearch")) {
      modules.add("v2/elasticsearch-common");
    }
    if (pomPath.contains("bigtable")) {
      modules.add("v2/bigtable-common");
    }

    modules.add(pomPath.substring(pomPath.indexOf("v2/")).replace("/pom.xml", ""));

    return modules;
  }

  @After
  public void tearDownBase() {
    if (gcsClient != null) {
      gcsClient.cleanupAll();
    }
  }

  protected PipelineLauncher launcher() {
    if (System.getProperty("directRunnerTest") != null) {
      return DirectRunnerClient.builder(templateClass).setCredentials(credentials).build();
    } else if (template.flexContainerName() != null && !template.flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(credentials).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(credentials).build();
    }
  }

  /**
   * Launch the template job with the given options. By default, it will setup the hooks to avoid
   * jobs getting leaked.
   */
  protected LaunchInfo launchTemplate(LaunchConfig.Builder options) throws IOException {
    return this.launchTemplate(options, true);
  }

  /**
   * Launch the template with the given options and configuration for hook.
   *
   * @param options Options to use for launch.
   * @param setupShutdownHook Whether should setup a hook to cancel the job upon VM termination.
   *     This is useful to teardown resources if the VM/test terminates unexpectedly.
   * @return Job details.
   * @throws IOException Thrown when {@link PipelineLauncher#launch(String, String, LaunchConfig)}
   *     fails.
   */
  protected LaunchInfo launchTemplate(LaunchConfig.Builder options, boolean setupShutdownHook)
      throws IOException {

    // Property allows testing with Runner v2 / Unified Worker
    if (System.getProperty("unifiedWorker") != null) {
      options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    }
    // Property allows testing with Streaming Engine Enabled
    if (System.getProperty("enableStreamingEngine") != null) {
      options.addEnvironment("enableStreamingEngine", true);
    }

    if (System.getProperty("workerMachineType") != null) {
      options.addEnvironment("workerMachineType", System.getProperty("workerMachineType"));
    }

    if (System.getProperty("directRunnerTest") != null) {
      // For direct runner tests we need to explicitly add a tempLocation if missing
      if (options.getParameter("tempLocation") == null) {
        options.addParameter("tempLocation", "gs://" + artifactBucketName + "/temp/");
      }

      if (System.getProperty("runner") != null) {
        options.addParameter("runner", System.getProperty("runner"));
      }

      // If template has creation parameters, they need to be specified as a --parameter=value
      for (Method method : template.optionsClass().getMethods()) {
        TemplateCreationParameters creationParameters =
            method.getAnnotation(TemplateCreationParameters.class);
        if (creationParameters != null) {
          for (TemplateCreationParameter param : creationParameters.value()) {
            if (param.template() == null
                || param.template().isEmpty()
                || param.template().equals(template.name())) {
              options.addParameter(
                  MetadataUtils.getParameterNameFromMethod(method.getName()), param.value());
            }
          }
        }
      }
    }

    PipelineLauncher pipelineLauncher = launcher();
    LaunchInfo launchInfo = pipelineLauncher.launch(PROJECT, REGION, options.build());

    // if the launch succeeded and setupShutdownHook is enabled, setup a thread to cancel job
    if (setupShutdownHook && launchInfo.jobId() != null && !launchInfo.jobId().isEmpty()) {
      Runtime.getRuntime()
          .addShutdownHook(new Thread(new CancelJobShutdownHook(pipelineLauncher, launchInfo)));
    }

    return launchInfo;
  }

  /** Get the Cloud Storage base path for this test suite. */
  protected String getGcsBasePath() {
    return getFullGcsPath(artifactBucketName, getClass().getSimpleName(), gcsClient.runId());
  }

  /** Get the Cloud Storage base path for a specific test. */
  protected String getGcsPath(TestName testName) {
    return getGcsPath(testName.getMethodName());
  }

  /** Get the Cloud Storage base path for a specific testing method or artifact id. */
  protected String getGcsPath(String artifactId) {
    return getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), gcsClient.runId(), artifactId);
  }

  /** Create the default configuration {@link PipelineOperator.Config} for a specific job info. */
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    Config.Builder configBuilder =
        Config.builder().setJobId(info.jobId()).setProject(PROJECT).setRegion(REGION);

    // For DirectRunner tests, reduce the max time and the interval, as there is no worker required
    if (System.getProperty("directRunnerTest") != null) {
      configBuilder =
          configBuilder.setTimeoutAfter(Duration.ofMinutes(3)).setCheckAfter(Duration.ofSeconds(5));
    }

    return configBuilder.build();
  }

  /**
   * Convert a BigQuery TableId to a table spec string.
   *
   * @param table TableId to format.
   * @return String in the format {project}.{dataset}.{table}.
   */
  public static String toTableSpecStandard(TableId table) {
    return String.format(
        "%s.%s.%s",
        table.getProject() != null ? table.getProject() : PROJECT,
        table.getDataset(),
        table.getTable());
  }

  /**
   * Convert a BigQuery TableId to a table spec string.
   *
   * @param table TableId to format.
   * @return String in the format {project}:{dataset}.{table}.
   */
  public static String toTableSpecLegacy(TableId table) {
    return String.format(
        "%s:%s.%s",
        table.getProject() != null ? table.getProject() : PROJECT,
        table.getDataset(),
        table.getTable());
  }

  protected PipelineOperator pipelineOperator() {
    return new PipelineOperator(launcher());
  }

  /**
   * This {@link Runnable} class calls {@link PipelineLauncher#cancelJob(String, String, String)}
   * for a specific instance of client and given job information, which is useful to enforcing
   * resource termination using {@link Runtime#addShutdownHook(Thread)}.
   */
  static class CancelJobShutdownHook implements Runnable {

    private final PipelineLauncher pipelineLauncher;
    private final LaunchInfo launchInfo;

    public CancelJobShutdownHook(PipelineLauncher pipelineLauncher, LaunchInfo launchInfo) {
      this.pipelineLauncher = pipelineLauncher;
      this.launchInfo = launchInfo;
    }

    @Override
    public void run() {
      try {
        Job cancelled =
            pipelineLauncher.cancelJob(
                launchInfo.projectId(), launchInfo.region(), launchInfo.jobId());
        LOG.warn("Job {} was shutdown by the hook to prevent resources leak.", cancelled.getId());
      } catch (Exception e) {
        // expected that the cancel fails if the test works as intended, so logging as debug only.
        LOG.debug("Error shutting down job {}: {}", launchInfo.jobId(), e.getMessage());
      }
    }
  }
}
