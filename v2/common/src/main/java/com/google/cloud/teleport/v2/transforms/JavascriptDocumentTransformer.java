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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Document UDF Transform Function. Note that this class's implementation is not threadsafe */
@AutoValue
public abstract class JavascriptDocumentTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(JavascriptDocumentTransformer.class);

  /**
   * Grabs code from a FileSystem, loads it into the Nashorn Javascript Engine, and executes
   * Javascript Functions.
   */
  @AutoValue
  public abstract static class JavascriptRuntime {
    @Nullable
    public abstract String fileSystemPath();

    @Nullable
    public abstract String functionName();

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
     *
     * @return a JavascriptTextTransformer builder
     */
    public static Builder newBuilder() {
      return new AutoValue_JavascriptDocumentTransformer_JavascriptRuntime.Builder();
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

      if (engine == null) {
        List<String> availableEngines = new ArrayList<>();
        for (ScriptEngineFactory factory : manager.getEngineFactories()) {
          availableEngines.add(factory.getEngineName() + " " + factory.getEngineVersion());
        }
        throw new RuntimeException(
            String.format("JavaScript engine not available. Found engines: %s.", availableEngines));
      }

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
    public Document invoke(Document data)
        throws ScriptException, IOException, NoSuchMethodException {
      Invocable invocable = getInvocable();
      if (invocable == null) {
        throw new RuntimeException("No udf was loaded");
      }

      Object result = getInvocable().invokeFunction(functionName(), data);
      if (result == null || ScriptObjectMirror.isUndefined(result)) {
        return null;
      } else if (result instanceof Document) {
        return (Document) result;
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
     * @return a collection of scripts loaded as UF8 Strings
     */
    private static Collection<String> getScripts(String path) throws IOException {
      MatchResult result = FileSystems.match(path);
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + path);

      List<String> scripts =
          result.metadata().stream()
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
      return scripts;
    }
  }

  /** Convert Document to table row. * */
  @AutoValue
  public abstract static class TransformDocumentViaJavascript
      extends PTransform<PCollection<Document>, PCollection<Document>> {
    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String functionName();

    /** Builder for {@link TransformDocumentViaJavascript}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract TransformDocumentViaJavascript build();
    }

    public static Builder newBuilder() {
      return new AutoValue_JavascriptDocumentTransformer_TransformDocumentViaJavascript.Builder();
    }

    @Override
    public PCollection<Document> expand(PCollection<Document> doc) {
      return doc.apply(
          ParDo.of(
              new DoFn<Document, Document>() {
                private JavascriptRuntime javascriptRuntime;

                @Setup
                public void setup() {
                  if (fileSystemPath() != null && functionName() != null) {
                    javascriptRuntime = getJavascriptRuntime(fileSystemPath(), functionName());
                  }
                }

                @ProcessElement
                public void processElement(ProcessContext c)
                    throws IOException, NoSuchMethodException, ScriptException {
                  Document element = c.element();
                  if (javascriptRuntime != null) {
                    element = javascriptRuntime.invoke(element);
                    c.output(element);
                  } else {
                    c.output(element);
                  }
                }
              }));
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
      String fileSystemPath, String functionName) {
    JavascriptRuntime javascriptRuntime = null;

    if (!Strings.isNullOrEmpty(fileSystemPath) && !Strings.isNullOrEmpty(functionName)) {
      javascriptRuntime =
          JavascriptRuntime.newBuilder()
              .setFunctionName(functionName)
              .setFileSystemPath(fileSystemPath)
              .build();
    }

    return javascriptRuntime;
  }
}
