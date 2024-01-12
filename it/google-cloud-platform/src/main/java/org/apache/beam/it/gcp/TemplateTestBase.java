/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.JobState;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.dataflow.ClassicTemplateClient;
import org.apache.beam.it.gcp.dataflow.DirectRunnerClient;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
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
  public static final String ADDITIONAL_EXPERIMENTS_ENVIRONMENT = "additionalExperiments";

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

  protected String specPath;
  protected Credentials credentials;
  protected CredentialsProvider credentialsProvider;
  protected String artifactBucketName;
  protected String testId;

  /** Cache to avoid staging the same template multiple times on the same execution. */
  private static final Cache<String, String> stagedTemplates = CacheBuilder.newBuilder().build();

  protected Template template;
  private Class<?> templateClass;

  /** Client to interact with GCS. */
  protected GcsResourceManager gcsClient;

  protected GcsResourceManager artifactClient;

  private boolean usingDirectRunner;
  protected PipelineLauncher pipelineLauncher;
  protected boolean skipBaseCleanup;

  @Before
  public void setUpBase() throws ExecutionException {

    testId = PipelineUtils.createJobName("test", 10);

    TemplateIntegrationTest annotation = null;
    usingDirectRunner = System.getProperty("directRunnerTest") != null;
    try {
      Method testMethod = getClass().getMethod(testName);
      annotation = testMethod.getAnnotation(TemplateIntegrationTest.class);
      Category category = testMethod.getAnnotation(Category.class);
      if (category != null) {
        usingDirectRunner =
            Arrays.asList(category.value()).contains(DirectRunnerTest.class) || usingDirectRunner;
      }
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
    if (template.placeholderClass() != void.class) {
      templateClass = template.placeholderClass();
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
      gcsClient =
          GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
              .build();

      // Keep name compatibility, for now
      artifactClient = gcsClient;

    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. Storage Client will not be"
              + " created automatically.");
    }

    credentialsProvider = FixedCredentialsProvider.create(credentials);
    pipelineLauncher = buildLauncher();

    if (usingDirectRunner) {
      // Using direct runner, not needed to stage.
      return;
    }

    if (TestProperties.specPath() != null && !TestProperties.specPath().isEmpty()) {
      LOG.info("A spec path was given, not staging template {}", template.name());
      specPath = TestProperties.specPath();
    } else {
      specPath =
          stagedTemplates.get(
              template.name(),
              () -> {
                LOG.info("Preparing test for {} ({})", template.name(), templateClass);

                String prefix =
                    new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + "_IT";

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
                      template.flexContainerName() != null
                          && !template.flexContainerName().isEmpty();
                  return String.format(
                      "gs://%s/%s/%s%s", bucketName, prefix, flex ? "flex/" : "", template.name());

                } catch (Exception e) {
                  throw new IllegalArgumentException("Error staging template", e);
                }
              });
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
      moduleBuild = String.join(",", List.of("metadata", "v1"));
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
      "-am",
      "-PtemplatesStage,pluginOutputDir,splunkDeps",
      "-DpluginRunId=" + RandomStringUtils.randomAlphanumeric(16),
      // Skip shading for now due to flakiness / slowness in the process.
      "-DskipShade",
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
      "-DtemplateName=" + template.name(),
      "-DunifiedWorker=" + System.getProperty("unifiedWorker"),
      // Print stacktrace when command fails
      "-e"
    };
  }

  private List<String> getModulesBuild(String pomPath) {
    List<String> modules = new ArrayList<>();
    modules.add("metadata");
    modules.add("v2/common");
    modules.add(pomPath.substring(pomPath.indexOf("v2/")).replace("/pom.xml", ""));

    return modules;
  }

  @After
  public void baseCleanup() throws IOException {
    if (!skipBaseCleanup) {
      tearDownBase();
    }
  }

  public void tearDownBase() throws IOException {
    LOG.info("Invoking tearDownBase cleanups for {} - {}", testName, testId);

    if (pipelineLauncher != null) {
      pipelineLauncher.cleanupAll();
    } else {
      LOG.error("pipelineLauncher was not initialized, there was an error triggering {}", testName);
    }
    if (gcsClient != null) {
      gcsClient.cleanupAll();
    }
  }

  protected PipelineLauncher buildLauncher() {
    if (usingDirectRunner) {
      return DirectRunnerClient.builder(templateClass).setCredentials(credentials).build();
    } else if (template.flexContainerName() != null && !template.flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder(credentials).build();
    } else {
      return ClassicTemplateClient.builder(credentials).build();
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

    boolean flex = template.flexContainerName() != null && !template.flexContainerName().isEmpty();

    // Property allows testing with Runner v2 / Unified Worker
    if (System.getProperty("unifiedWorker") != null) {
      appendExperiment(options, "use_runner_v2");

      if (System.getProperty("sdkContainerImage") != null) {
        options.addParameter("sdkContainerImage", System.getProperty("sdkContainerImage"));
        appendExperiment(
            options, "worker_harness_container_image=" + System.getProperty("sdkContainerImage"));
        appendExperiment(options, "disable_worker_rolling_upgrade");
      }
    }

    if (System.getProperty("enableCleanupState") != null) {
      appendExperiment(options, "enable_cleanup_state");
    }

    // Property allows testing with Streaming Engine Enabled
    if (System.getProperty("enableStreamingEngine") != null) {
      options.addEnvironment("enableStreamingEngine", true);
    }

    if (System.getProperty("workerMachineType") != null) {
      options.addEnvironment("workerMachineType", System.getProperty("workerMachineType"));
    }

    if (System.getProperty("streamingAtLeastOnce") != null) {
      appendExperiment(options, "streaming_mode_at_least_once");
    }

    if (usingDirectRunner) {
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
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), gcsClient.runId());
  }

  /** Get the Cloud Storage base path for a specific test. */
  protected String getGcsPath(TestName testName) {
    return getGcsPath(testName.getMethodName());
  }

  /** Get the Cloud Storage base path for a specific testing method or artifact id. */
  protected String getGcsPath(String artifactId) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), gcsClient.runId(), artifactId);
  }

  /** Create the default configuration {@link PipelineOperator.Config} for a specific job info. */
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return createConfig(info, null);
  }

  /** Create the default configuration {@link PipelineOperator.Config} for a specific job info. */
  protected PipelineOperator.Config createConfig(LaunchInfo info, Duration duration) {
    Config.Builder configBuilder =
        Config.builder().setJobId(info.jobId()).setProject(PROJECT).setRegion(REGION);

    if (duration != null) {
      configBuilder = configBuilder.setTimeoutAfter(duration);
    }

    // For DirectRunner tests, reduce the max time and the interval, as there is no worker required
    if (usingDirectRunner) {
      configBuilder =
          configBuilder.setTimeoutAfter(Duration.ofMinutes(4)).setCheckAfter(Duration.ofSeconds(5));
    }

    return wrapConfiguration(configBuilder).build();
  }

  /** Entrypoint allowed to override settings. */
  protected Config.Builder wrapConfiguration(Config.Builder builder) {
    return builder;
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

  /**
   * Append experiment to the given launch options.
   *
   * @param options Launch Options to extend.
   * @param experiment Experiment to add to the list.
   */
  protected void appendExperiment(LaunchConfig.Builder options, String experiment) {
    Object additionalExperiments = options.getEnvironment(ADDITIONAL_EXPERIMENTS_ENVIRONMENT);
    if (additionalExperiments == null) {
      options.addEnvironment(
          ADDITIONAL_EXPERIMENTS_ENVIRONMENT, Collections.singletonList(experiment));
    } else if (additionalExperiments instanceof Collection) {
      List<String> list = new ArrayList<>((Collection<String>) additionalExperiments);
      list.add(experiment);

      options.addEnvironment(ADDITIONAL_EXPERIMENTS_ENVIRONMENT, list);
    } else {
      throw new IllegalStateException("Invalid additionalExperiments " + additionalExperiments);
    }
  }

  protected PipelineOperator pipelineOperator() {
    return new PipelineOperator(pipelineLauncher);
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
      // Ignore shutdown hook
      if (JobState.FINISHING_STATES.contains(launchInfo.state())) {
        return;
      }
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
