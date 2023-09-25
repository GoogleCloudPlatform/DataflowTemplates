/*
 * Copyright (C) 2019 Google LLC
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharStreams;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Text UDF Transform Function. Note that this class's implementation is not threadsafe */
@AutoValue
public abstract class JavascriptTextTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(JavascriptTextTransformer.class);

  /** Necessary CLI options for running UDF function. */
  public interface JavascriptTextTransformerOptions extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = true,
        description = "Cloud Storage path to Javascript UDF source",
        helpText =
            "The Cloud Storage path pattern for the JavaScript code containing your user-defined "
                + "functions.",
        example = "gs://your-bucket/your-function.js")
    String getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(String javascriptTextTransformGcsPath);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[a-zA-Z0-9_]+"},
        description = "UDF Javascript Function Name",
        helpText =
            "The name of the function to call from your JavaScript file. Use only letters, digits, and underscores.",
        example = "'transform' or 'transform_udf1'")
    String getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);

    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "JavaScript UDF auto-reload interval (minutes)",
        helpText =
            "Define the interval that workers may check for JavaScript UDF changes to reload the files.")
    @Default.Integer(0)
    Integer getJavascriptTextTransformReloadIntervalMinutes();

    void setJavascriptTextTransformReloadIntervalMinutes(
        Integer javascriptTextTransformReloadIntervalMinutes);
  }

  /**
   * Grabs code from a FileSystem, loads it into the Nashorn Javascript Engine, and executes
   * Javascript Functions.
   */
  @AutoValue
  public abstract static class JavascriptRuntime {

    /** JavaScript Engines to look for in the classpath. */
    private static final List<String> JAVASCRIPT_ENGINE_NAMES =
        Arrays.asList("Nashorn", "JavaScript");

    @Nullable
    public abstract String fileSystemPath();

    @Nullable
    public abstract String functionName();

    @Nullable
    public abstract Integer reloadIntervalMinutes();

    private static final Distribution JAVASCRIPT_RELOAD_LATENCY_MS =
        Metrics.distribution(JavascriptTextTransformer.class, "javascript_reload_latency_ms");

    private static LoadingCache<JavascriptRuntime, Invocable> cache =
        Caffeine.newBuilder()
            .expireAfter(
                new Expiry<JavascriptRuntime, Invocable>() {
                  public long expireAfterCreate(
                      JavascriptRuntime runtime, Invocable invocable, long currentTime) {
                    // Do not expire if reload is disabled
                    if (runtime.reloadIntervalMinutes() == null
                        || runtime.reloadIntervalMinutes() <= 0) {
                      return Long.MAX_VALUE;
                    }
                    return TimeUnit.MINUTES.toNanos(runtime.reloadIntervalMinutes());
                  }

                  public long expireAfterUpdate(
                      JavascriptRuntime runtime,
                      Invocable invocable,
                      long currentTime,
                      long currentDuration) {
                    return currentDuration;
                  }

                  public long expireAfterRead(
                      JavascriptRuntime runtime,
                      Invocable invocable,
                      long currentTime,
                      long currentDuration) {
                    return currentDuration;
                  }
                })
            .build(runtime -> buildInvocable(runtime));

    private Instant lastRefreshCheck = Instant.now();

    /** Builder for {@link JavascriptTextTransformer}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract Builder setReloadIntervalMinutes(@Nullable Integer value);

      public abstract JavascriptRuntime build();
    }

    /**
     * Factory method for generating a JavascriptTextTransformer.Builder.
     *
     * @return a JavascriptTextTransformer builder
     */
    public static Builder newBuilder() {
      return new AutoValue_JavascriptTextTransformer_JavascriptRuntime.Builder();
    }

    /**
     * Gets a cached Javascript Invocable, if fileSystemPath() not set, returns null.
     *
     * @return a Javascript Invocable or null
     */
    @Nullable
    public Invocable getInvocable() throws ScriptException, IOException {

      // return null if no UDF path specified.
      if (Strings.isNullOrEmpty(fileSystemPath())) {
        return null;
      }

      return cache.get(this);
    }

    /**
     * Factory method for making a new Invocable.
     *
     * @param scripts a collection of javascript scripts encoded with UTF8 to load in
     */
    private static Invocable newInvocable(Collection<String> scripts) throws ScriptException {
      long startTime = Instant.now().toEpochMilli();
      ScriptEngine engine = getJavaScriptEngine();
      for (String script : scripts) {
        engine.eval(script);
      }
      JAVASCRIPT_RELOAD_LATENCY_MS.update(Instant.now().toEpochMilli() - startTime);
      return (Invocable) engine;
    }

    private static Invocable buildInvocable(JavascriptRuntime runtime)
        throws IOException, ScriptException {
      // List of all scripts read from the filesystem
      Collection<String> scripts = getScripts(runtime.fileSystemPath());
      return newInvocable(scripts);
    }

    private static ScriptEngine getJavaScriptEngine() {
      NashornScriptEngineFactory nashornFactory = new NashornScriptEngineFactory();
      ScriptEngine engine = nashornFactory.getScriptEngine("--language=es6");

      if (engine != null) {
        return engine;
      }

      List<String> availableEngines = new ArrayList<>();
      ScriptEngineManager manager = new ScriptEngineManager();
      for (ScriptEngineFactory factory : manager.getEngineFactories()) {
        availableEngines.add(
            factory.getEngineName()
                + " ("
                + factory.getEngineVersion()
                + ") - "
                + factory.getNames());
      }

      throw new RuntimeException(
          String.format("JavaScript engine not available. Found engines: %s.", availableEngines));
    }

    /**
     * Invokes the UDF with specified data.
     *
     * @param data data to pass to the invocable function
     * @return The data transformed by the UDF in String format
     */
    @Nullable
    public String invoke(String data) throws ScriptException, IOException, NoSuchMethodException {
      Invocable invocable = getInvocable();
      if (invocable == null) {
        throw new RuntimeException("No UDF was loaded");
      }

      Object result = invocable.invokeFunction(functionName(), data);
      if (result == null || ScriptObjectMirror.isUndefined(result)) {
        return null;
      } else if (result instanceof String) {
        return (String) result;
      } else {
        String className = result.getClass().getName();
        throw new RuntimeException(
            "UDF Function did not return a String. Instead got: " + className);
      }
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

      return result.metadata().stream()
          .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
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
    }
  }

  /** Transforms Text Strings via a Javascript UDF. */
  @AutoValue
  public abstract static class TransformTextViaJavascript
      extends PTransform<PCollection<String>, PCollection<String>> {
    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String functionName();

    public abstract @Nullable Integer reloadIntervalMinutes();

    /** Builder for {@link TransformTextViaJavascript}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract Builder setReloadIntervalMinutes(@Nullable Integer value);

      public abstract TransformTextViaJavascript build();
    }

    public static Builder newBuilder() {
      return new AutoValue_JavascriptTextTransformer_TransformTextViaJavascript.Builder();
    }

    @Override
    public PCollection<String> expand(PCollection<String> strings) {
      return strings.apply(
          ParDo.of(
              new DoFn<String, String>() {
                private JavascriptRuntime javascriptRuntime;

                @Setup
                public void setup() {
                  if (fileSystemPath() != null && functionName() != null) {
                    javascriptRuntime =
                        getJavascriptRuntime(
                            fileSystemPath(),
                            functionName(),
                            reloadIntervalMinutes() != null ? reloadIntervalMinutes() : null);
                  }
                }

                @ProcessElement
                public void processElement(ProcessContext c)
                    throws IOException, NoSuchMethodException, ScriptException {
                  String element = c.element();

                  if (javascriptRuntime != null) {
                    element = javascriptRuntime.invoke(element);
                  }

                  if (!Strings.isNullOrEmpty(element)) {
                    c.output(element);
                  }
                }
              }));
    }
  }

  /**
   * The {@link FailsafeJavascriptUdf} class processes user-defined functions is a fail-safe manner
   * by maintaining the original payload post-transformation and outputting to a dead-letter on
   * failure.
   */
  @AutoValue
  public abstract static class FailsafeJavascriptUdf<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {
    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String functionName();

    public abstract @Nullable Integer reloadIntervalMinutes();

    public abstract @Nullable Boolean loggingEnabled();

    public abstract TupleTag<FailsafeElement<T, String>> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_JavascriptTextTransformer_FailsafeJavascriptUdf.Builder<>();
    }

    private final Counter successCounter =
        Metrics.counter(FailsafeJavascriptUdf.class, "udf-transform-success-count");

    private final Counter failedCounter =
        Metrics.counter(FailsafeJavascriptUdf.class, "udf-transform-failed-count");

    /** Builder for {@link FailsafeJavascriptUdf}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {
      public abstract Builder<T> setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder<T> setFunctionName(@Nullable String functionName);

      public abstract Builder<T> setReloadIntervalMinutes(Integer value);

      public abstract Builder<T> setLoggingEnabled(@Nullable Boolean loggingEnabled);

      public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafeJavascriptUdf<T> build();
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
      return elements.apply(
          "ProcessUdf",
          ParDo.of(
                  new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {
                    private JavascriptRuntime javascriptRuntime;
                    private boolean loggingEnabled;

                    @Setup
                    public void setup() {
                      if (fileSystemPath() != null && functionName() != null) {
                        javascriptRuntime =
                            getJavascriptRuntime(
                                fileSystemPath(),
                                functionName(),
                                reloadIntervalMinutes() != null ? reloadIntervalMinutes() : null);
                      }

                      if (loggingEnabled() != null) {
                        loggingEnabled = loggingEnabled();
                      }
                    }

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      FailsafeElement<T, String> element = context.element();
                      String payloadStr = element.getPayload();

                      try {
                        if (javascriptRuntime != null) {
                          payloadStr = javascriptRuntime.invoke(payloadStr);
                        }

                        if (!Strings.isNullOrEmpty(payloadStr)) {
                          context.output(
                              FailsafeElement.of(element.getOriginalPayload(), payloadStr));
                          successCounter.inc();
                        }
                      } catch (Throwable e) {
                        // Throwable caught because UDFS can trigger Errors (e.g., StackOverflow)
                        if (loggingEnabled) {
                          LOG.warn(
                              "Exception occurred while applying UDF '{}' from file path '{}' due"
                                  + " to '{}'",
                              functionName(),
                              fileSystemPath(),
                              e.getMessage());
                        }

                        context.output(
                            failureTag(),
                            FailsafeElement.of(element)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                        failedCounter.inc();
                      }
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }
  }

  /**
   * Retrieves a {@link JavascriptRuntime} configured to invoke the specified function within the
   * script. If either the fileSystemPath or functionName is null or empty, this method will return
   * null indicating that a runtime was unable to be created within the given parameters.
   *
   * @param fileSystemPath The file path to the JavaScript file to execute.
   * @param functionName The function name which will be invoked within the JavaScript script.
   * @return The {@link JavascriptRuntime} instance.
   */
  private static JavascriptRuntime getJavascriptRuntime(
      String fileSystemPath, String functionName, Integer reloadIntervalMinutes) {
    JavascriptRuntime javascriptRuntime = null;

    if (!Strings.isNullOrEmpty(fileSystemPath) && !Strings.isNullOrEmpty(functionName)) {
      javascriptRuntime =
          JavascriptRuntime.newBuilder()
              .setFunctionName(functionName)
              .setFileSystemPath(fileSystemPath)
              .setReloadIntervalMinutes(reloadIntervalMinutes)
              .build();
    }

    return javascriptRuntime;
  }
}
