/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A Text UDF Transform Function.
 * Note that this class's implementation is not threadsafe
 */
@AutoValue
public abstract class JavascriptTextTransformer {

  /**
   * Necessary CLI options for running UDF function.
   */
  public interface JavascriptTextTransformerOptions extends PipelineOptions {
    @Description("Gcs path to javascript udf source")
    ValueProvider<String> getJavascriptTextTransformGcsPath();
    void setJavascriptTextTransformGcsPath(ValueProvider<String> javascriptTextTransformGcsPath);

    @Description("UDF Javascript Function Name")
    ValueProvider<String> getJavascriptTextTransformFunctionName();
    void setJavascriptTextTransformFunctionName(
        ValueProvider<String> javascriptTextTransformFunctionName);
  }

  /**
   * Grabs code from a FileSystem, loads it into the Nashorn Javascript Engine, and
   * executes Javascript Functions.
   */
  @AutoValue
  public abstract static class JavascriptRuntime {
    @Nullable public abstract String fileSystemPath();
    @Nullable public abstract String functionName();
    private Invocable invocable;

    /** Builder for {@link JavascriptTextTransformer}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);
      public abstract Builder setFunctionName(@Nullable String functionName);
      public abstract JavascriptRuntime build();
    }

    /**
     * Factory method for generating a JavascriptTextTransformer.Builder.
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
    public Invocable getInvocable()
        throws ScriptException, IOException {

      // return null if no UDF path specified.
      if (Strings.isNullOrEmpty(fileSystemPath())) {
        return null;
      }

      if (invocable == null) {
        Collection<String> scripts = getScripts(fileSystemPath());
        invocable = newInvocable(scripts);
      }
      return invocable;
    }

    /**
     * Factory method for making a new Invocable.
     *
     * @param scripts a collection of javascript scripts encoded with UTF8 to load in
     */
    @Nullable
    private static Invocable newInvocable(Collection<String> scripts) throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("JavaScript");

      for (String script : scripts) {
        engine.eval(script);
      }

      return (Invocable) engine;
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
        throw new RuntimeException("No udf was loaded");
      }

      Object result = getInvocable().invokeFunction(functionName(), data);
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
     * Loads into memory scripts from a File System from a given path.
     * Supports any file system that {@link FileSystems} supports.
     *
     * @return a collection of scripts loaded as UF8 Strings
     */
    private static Collection<String> getScripts(String path) throws IOException {
      MatchResult result = FileSystems.match(path);
      checkArgument(result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + path);

      List<String> scripts = result
          .metadata()
          .stream()
          .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
          .map(Metadata::resourceId)
          .map(resourceId -> {
            try (Reader reader = Channels.newReader(
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

  /** Transforms Text Strings via a Javascript UDF. */
  @AutoValue
  public abstract static class TransformTextViaJavascript
      extends PTransform<PCollection<String>, PCollection<String>> {
    public abstract @Nullable ValueProvider<String> fileSystemPath();
    public abstract @Nullable ValueProvider<String> functionName();

    /** Builder for {@link TransformTextViaJavascript}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable ValueProvider<String> fileSystemPath);
      public abstract Builder setFunctionName(@Nullable ValueProvider<String> functionName);
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
                    if (!Strings.isNullOrEmpty(fileSystemPath().get())
                        && !Strings.isNullOrEmpty(functionName().get())) {
                      javascriptRuntime =
                          JavascriptRuntime.newBuilder()
                              .setFunctionName(functionName().get())
                              .setFileSystemPath(fileSystemPath().get())
                              .build();
                    }
                  }
                }

                @ProcessElement
                public void processElement(ProcessContext c)
                    throws GeneralSecurityException, IOException, NoSuchMethodException,
                        ScriptException {
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
}
