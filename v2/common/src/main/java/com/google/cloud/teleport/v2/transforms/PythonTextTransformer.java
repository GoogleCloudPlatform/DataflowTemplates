/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Text UDF Transform Function. Note that this class's implementation is not threadsafe */
@AutoValue
public abstract class PythonTextTransformer implements Serializable {

  public static final String DEFAULT_PYTHON_VERSION = "python3";

  private static final Logger LOG = LoggerFactory.getLogger(PythonTextTransformer.class);

  /** Necessary CLI options for running UDF function. */
  public interface PythonTextTransformerOptions extends PipelineOptions {

    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = true,
        description = "Gcs path to python UDF source",
        helpText =
            "The Cloud Storage path pattern for the Python code containing your user-defined functions.",
        example = "gs://your-bucket/your-transforms/*.py")
    String getPythonTextTransformGcsPath();

    void setPythonTextTransformGcsPath(String pythonTextTransformGcsPath);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "Python UDF Runtime Version",
        helpText = "The runtime version to use for this Python UDF.")
    String getPythonRuntimeVersion();

    void setPythonRuntimeVersion(String pythonRuntimeVersion);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "UDF Python Function Name",
        helpText =
            "The name of the function to call from your JavaScript file. Use only letters, digits, and underscores.",
        example = "transform_udf1")
    String getPythonTextTransformFunctionName();

    void setPythonTextTransformFunctionName(String pythonTextTransformFunctionName);

    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "Python runtime retry attempts",
        helpText = "The number of times a runtime will be retried before failing.")
    @Default.Integer(5)
    Integer getRuntimeRetries();

    void setRuntimeRetries(Integer runtimeRetries);
  }

  /** Grabs code from a FileSystem, loads into ProcessBuilder. */
  @AutoValue
  public abstract static class PythonRuntime {

    @Nullable
    public abstract String fileSystemPath();

    @Nullable
    public abstract String runtimeVersion();

    @Nullable
    public abstract String functionName();

    @Nullable
    public abstract Integer runtimeRetries();

    private ProcessBuilder process;

    private Process runtime;

    private Process installRuntime;
    private Boolean pythonWasBuilt = false;
    private final ReentrantLock pythonInstallLock = new ReentrantLock();
    private static String missingPythonErrorMessage = "Cannot run program \"python";

    /** Builder for {@link PythonTextTransformer}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setRuntimeVersion(@Nullable String runtimeVersion);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract Builder setRuntimeRetries(@Nullable Integer runtimeRetries);

      public abstract PythonRuntime build();
    }

    /**
     * Factory method for generating a PythonTextTransformer.Builder.
     *
     * @return a PythonTextTransformer builder
     */
    public static Builder newBuilder() {
      return new AutoValue_PythonTextTransformer_PythonRuntime.Builder();
    }

    /**
     * Gets a cached Javascript Invocable, if fileSystemPath() not set, returns null. NEED
     * REPLACEMENT FOR INVOCABLE
     *
     * @return a python Invocable or null
     */
    @Nullable
    public ProcessBuilder getProcessBuilder() throws IOException {

      // return null if no UDF path specified.
      if (Strings.isNullOrEmpty(fileSystemPath())) {
        throw new IllegalArgumentException("Python UDF Transform: no file provided.");
      }

      if (process == null) {
        Collection<String> scripts = getScripts(fileSystemPath());
        FileWriter writer = new FileWriter(functionName());
        if (scripts.size() == 0) {
          throw new IllegalArgumentException(
              String.format("Python UDF Transform: file {} not valid.", fileSystemPath()));
        }
        for (String str : scripts) {
          writer.write(str + System.lineSeparator());
        }
        writer.close();

        process = newProcess();
      }
      return process;
    }

    /** Factory method for making a new Invocable. */
    @Nullable
    private static ProcessBuilder newProcess() {
      ProcessBuilder pb = new ProcessBuilder();

      return pb;
    }

    /**
     * Build Python Runtime Environment.
     *
     * @param pythonVersion The python runtime version to be built. ie python3
     * @return None
     */
    public void buildPythonExecutable(String pythonVersion)
        throws IOException, NoSuchMethodException, InterruptedException {
      // Sleep to create queues TODO use exponential backoff
      Thread.sleep(10000);

      // Creating the Process to Upgrade Apt
      LOG.info("Updating apt-get");
      // installRuntime = new ProcessBuilder().command("apt-get", "update").start();
      installRuntime =
          new ProcessBuilder()
              .command("flock", "-xn", "/tmp/apt.upgrade.lock", "apt-get", "update")
              .start();
      installRuntime.waitFor(120L, TimeUnit.SECONDS);
      installRuntime.destroy();

      // Creating the Process to Install Python(3)
      LOG.info("Installing or Upgrading Python");
      // installRuntime = new ProcessBuilder().command("apt-get", "upgrade", pythonVersion,
      // "-y").start();
      installRuntime =
          new ProcessBuilder()
              .command(
                  "flock", "-xn", "/tmp/apt.python.lock", "apt-get", "upgrade", pythonVersion, "-y")
              .start();
      installRuntime.waitFor(120L, TimeUnit.SECONDS);
      installRuntime.destroy();
    }

    /**
     * Invokes the UDF with specified list of data.
     *
     * @param data data to pass to the invocable function
     * @return The data transformed by the UDF in String format
     */
    @Nullable
    public List<String> invoke(File file, Integer retries)
        throws IOException, NoSuchMethodException, InterruptedException {
      // Save Data in Temporary File
      LOG.info("Applying runtime");

      // Apply Python
      List<String> results = applyRuntimeToFile(file, retries);

      return results;
    }

    @Nullable
    public List<String> applyRuntimeToFile(File dataFile, Integer retries)
        throws IOException, NoSuchMethodException, InterruptedException {
      // Vars Required in function
      Process runtime;
      String pythonVersion = runtimeVersion();
      Integer retriesRemaining = retries - 1;
      String stderr;

      // Apply Python
      try {
        LOG.info("Apply Python to File: " + dataFile.getAbsolutePath());
        runtime =
            getProcessBuilder()
                .command(pythonVersion, functionName(), dataFile.getAbsolutePath())
                .start();
        LOG.info("Waiting For Results: " + dataFile.getAbsolutePath());
        if (retriesRemaining > 0) {
          if (runtime != null) {
            runtime.destroyForcibly();
          }
          return applyRuntimeToFile(dataFile, retriesRemaining);
        }
      } catch (IOException e) {
        LOG.info("IO Exception Seen");
        if (e.getMessage().startsWith(missingPythonErrorMessage)) {
          // Build Python and Retry
          buildPythonExecutable(pythonVersion);
          if (retriesRemaining > 0) {
            return applyRuntimeToFile(dataFile, retriesRemaining);
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      } catch (Exception e) {
        LOG.info("Non IO Exception Seen");
        throw e;
      }

      // Test Runtime Exists (should not be possible to hit this case)
      if (runtime == null) {
        throw new IOException("no runtime was provided");
      }

      // Process Python Results
      LOG.info("Process Python Results: " + dataFile.getAbsolutePath());
      List<String> results = new ArrayList<>();
      try {
        final BufferedReader reader =
            new BufferedReader(new InputStreamReader(runtime.getInputStream()));
        reader.lines().iterator().forEachRemaining(results::add);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        runtime.destroy();
      }

      return results;
    }

    /**
     * Loads into memory scripts from a File System from a given path. Supports any file system that
     * {@link FileSystems} supports.
     *
     * @return a collection of scripts loaded as UTF8 Strings
     */
    private static Collection<String> getScripts(String path) throws IOException {
      MatchResult result = FileSystems.match(path);
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + path);

      LOG.info("getting script!");

      List<String> scripts =
          result.metadata().stream()
              .filter(metadata -> metadata.resourceId().getFilename().endsWith(".py"))
              .map(Metadata::resourceId)
              .map(
                  resourceId -> {
                    try (Reader reader =
                        Channels.newReader(
                            FileSystems.open(resourceId), StandardCharsets.UTF_8.name())) {
                      return CharStreams.toString(reader);
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  })
              .collect(Collectors.toList());
      return scripts;
    }
  }

  /**
   * The {@link FailsafePythonUdf} class processes user-defined functions is a fail-safe manner by
   * maintaining the original payload post-transformation and outputting to a dead-letter on
   * failure.
   */
  @AutoValue
  public abstract static class FailsafePythonUdf<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String runtimeVersion();

    public abstract @Nullable String functionName();

    public abstract @Nullable Integer runtimeRetries();

    public abstract TupleTag<FailsafeElement<T, String>> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_PythonTextTransformer_FailsafePythonUdf.Builder<>();
    }

    private Counter successCounter =
        Metrics.counter(FailsafePythonUdf.class, "udf-transform-success-count");

    private Counter failedCounter =
        Metrics.counter(FailsafePythonUdf.class, "udf-transform-failed-count");

    private Counter thresholdCounter =
        Metrics.counter(FailsafePythonUdf.class, "udf-threshold-trigger");

    /** Builder for {@link FailsafePythonUdf}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder<T> setRuntimeVersion(@Nullable String runtimeVersion);

      public abstract Builder<T> setFunctionName(@Nullable String functionName);

      public abstract Builder<T> setRuntimeRetries(@Nullable Integer runtimeRetries);

      public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafePythonUdf<T> build();
    }

    private String getPythonVersion() {
      if (runtimeVersion() != null) {
        return runtimeVersion();
      } else {
        return DEFAULT_PYTHON_VERSION;
      }
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
      return elements.apply(
          "ProcessUdf",
          ParDo.of(
                  new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {
                    private PythonRuntime pythonRuntime;
                    private Integer batchLimit;
                    private Integer batchCounter;
                    private HashMap<String, FailsafeElement<T, String>> futures;
                    private String processUUID;
                    private File manifestFile;
                    private BufferedWriter dataWriter;
                    private BoundedWindow window;

                    @Setup
                    public void setup()
                        throws IOException, NoSuchMethodException, InterruptedException {
                      String runtimeVersion = getPythonVersion();

                      if (fileSystemPath() != null && functionName() != null) {
                        LOG.info("getting runtime!");
                        pythonRuntime =
                            getPythonRuntime(fileSystemPath(), functionName(), runtimeVersion);
                        LOG.info("Build Python Env for version {}", runtimeVersion);

                        pythonRuntime.buildPythonExecutable(runtimeVersion);
                      } else {
                        LOG.warn(
                            "Not setting up a Python Mapper runtime, because "
                                + "fileSystemPath={} and functionName={}",
                            fileSystemPath(),
                            functionName());
                        return;
                      }
                    }

                    @StartBundle
                    public void startBundle(StartBundleContext context) throws IOException {
                      batchCounter = 0;
                      batchLimit = 1000;
                      processUUID = UUID.randomUUID().toString();
                      manifestFile =
                          File.createTempFile(String.format("manifest_%s", processUUID), null);
                      futures = new HashMap<String, FailsafeElement<T, String>>();
                      dataWriter =
                          new BufferedWriter(new FileWriter(manifestFile.getAbsolutePath()));
                      LOG.info("file is at {}", manifestFile.getAbsolutePath());

                      // initialize a temp file (non local) and a counter
                      // TODO: createFile function

                    }

                    @ProcessElement
                    public void processElement(ProcessContext context, BoundedWindow window)
                        throws IOException {
                      this.window = window;
                      FailsafeElement<T, String> element = context.element();
                      String payloadStr = element.getPayload();
                      String eventId = UUID.randomUUID().toString();
                      JSONObject originalPayload = new JSONObject(payloadStr);
                      JSONObject json = new JSONObject();
                      json.put("id", eventId);
                      json.put("event", originalPayload);
                      String wrappedPayload = json.toString();
                      futures.put(eventId, element);
                      // TODO: add a counter of sum of total bytes
                      // 1) add event and increase counter
                      // 2) if counter > X process all of the rows

                      dataWriter.write(wrappedPayload);
                      dataWriter.newLine();
                      dataWriter.flush();
                      batchCounter++;
                    }

                    // TODO: create an execute batch fn

                    @FinishBundle
                    public void finishBundle(FinishBundleContext context)
                        throws IOException, NoSuchMethodException, InterruptedException {
                      LOG.info("closing batch at {} events", batchCounter);
                      Integer retries = runtimeRetries();
                      List<String> results = new ArrayList<>();
                      LOG.info("executing the batch!!!");
                      results = pythonRuntime.invoke(manifestFile, retries);
                      LOG.info("processed {} number of records", results.size());
                      for (int iter = 0; iter < results.size(); iter++) {
                        String event = results.get(iter);
                        JSONObject json = new JSONObject(event);
                        String eventId = json.getString("id");
                        FailsafeElement<T, String> originalEvent = futures.get(eventId);

                        // FIX THIS
                        if (json.getString("status").equals("SUCCESS")) {
                          String transformedEvent = json.getJSONObject("event").toString();
                          context.output(
                              FailsafeElement.of(
                                  originalEvent.getOriginalPayload(),
                                  json.getJSONObject("event").toString()),
                              window.maxTimestamp(),
                              window);
                          successCounter.inc();
                        } else if (json.getString("status").equals("FAILED")) {
                          context.output(
                              originalEvent
                                  .setErrorMessage(json.getString("error_message"))
                                  .setStacktrace(json.getString("error_message")),
                              window.maxTimestamp(),
                              window);
                        } else {
                          LOG.info("Failed to emit an event");
                          LOG.info("event status was {}", json.getString("status"));
                        }
                      }
                      futures.clear();
                      dataWriter.close();
                      manifestFile.delete();
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }
  }

  /**
   * Retrieves a {@link PythonRuntime} configured to invoke the specified function within the
   * script. If either the fileSystemPath or functionName is null or empty, this method will return
   * null indicating that a runtime was unable to be created within the given parameters.
   *
   * @param fileSystemPath The file path to the JavaScript file to execute.
   * @param functionName The function name which will be invoked within the JavaScript script.
   * @return The {@link PythonRuntime} instance.
   */
  private static PythonRuntime getPythonRuntime(
      String fileSystemPath, String functionName, String pythonVersion) {
    PythonRuntime runtime = null;

    if (!Strings.isNullOrEmpty(fileSystemPath) && !Strings.isNullOrEmpty(functionName)) {
      runtime =
          PythonRuntime.newBuilder()
              .setFunctionName(functionName)
              .setRuntimeVersion(pythonVersion)
              .setFileSystemPath(fileSystemPath)
              .build();
    }

    return runtime;
  }
}
