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

import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptRuntime;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.util.Arrays;
import java.util.List;
import javax.script.ScriptException;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/** Unit tests for {@link JavascriptTextTransformer}. */
@RunWith(JUnit4.class)
public class JavascriptTextTransformerTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String RESOURCES = "src/test/resources/"
      + "com/google/cloud/teleport/templates/common/JavascriptTextTransformerTest/";

  /**
   * Test {@link JavascriptRuntime#getInvocable} throws IllegalArgumentException
   * if bad filepath.
   * @throws Exception
   */
  @Test
  public void testInvokeThrowsException() throws Exception {
    JavascriptRuntime javascriptRuntime = JavascriptRuntime.newBuilder()
        .setFunctionName("transform")
        .setFileSystemPath("gs://some/bad/random/path")
        .build();

    thrown.expect(IllegalArgumentException.class);
    javascriptRuntime.getInvocable();
  }

  /**
   * Test @{link JavscriptRuntime#getInvocable} throws ScriptException if error in script.
   */
  @Test
  public void testInvokeScriptException() throws Exception {
    JavascriptRuntime javascriptRuntime = JavascriptRuntime.newBuilder()
        .setFileSystemPath(RESOURCES + "BadScripts/scriptParseException.js")
        .setFunctionName("transform")
        .build();
    thrown.expect(ScriptException.class);
    String data = javascriptRuntime.invoke("{\"answerToLife\": 42}");
  }

  /**
   * Test {@link JavascriptRuntime#invoke(String)} returns transformed data
   * when a good javascript transform function given.
   */
  @Test
  public void testInvokeGood() throws Exception {
    JavascriptRuntime javascriptRuntime = JavascriptRuntime.newBuilder()
        .setFileSystemPath(RESOURCES + "GoodScripts/transform.js")
        .setFunctionName("transform")
        .build();
    String data = javascriptRuntime.invoke("{\"answerToLife\": 42}");
    Assert.assertEquals("{\"answerToLife\":42,\"someProp\":\"someValue\"}", data);
  }

  /**
   * Test {@link JavascriptRuntime#invoke(String)} errors when undefined data
   * returned from javascript function.
   */
  @Test
  public void testInvokeUndefined() throws Exception {
    JavascriptRuntime javascriptRuntime = JavascriptRuntime.newBuilder()
        .setFileSystemPath(RESOURCES + "GoodScripts/returnsUndefined.js")
        .setFunctionName("transform")
        .build();
    String data = javascriptRuntime.invoke("{\"answerToLife\": 43}");
    Assert.assertNull(data);
  }

  /**
   * Test {@link TransformTextViaJavascript} returns transformed data
   * when a good javascript transform given.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnGood() throws Exception {
    List<String> inJson = Arrays.asList("{\"answerToLife\": 42}");
    List<String> expectedJson = Arrays.asList(
        "{\"answerToLife\":42,\"someProp\":\"someValue\"}");

    PCollection<String> transformedJson = pipeline
        .apply("Create", Create.of(inJson))
        .apply(TransformTextViaJavascript.newBuilder()
            .setFileSystemPath(StaticValueProvider.of(RESOURCES + "GoodScripts/*.js"))
            .setFunctionName(StaticValueProvider.of("transform"))
            .build());

    PAssert.that(transformedJson).containsInAnyOrder(expectedJson);

    pipeline.run();
  }

  /** Test {@link TransformTextViaJavascript} passes through data when empty strings as args. */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughEmptyStrings() throws Exception {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson = pipeline
        .apply("Create", Create.of(inJson))
        .apply(TransformTextViaJavascript.newBuilder()
            .setFunctionName(StaticValueProvider.of(""))
            .setFileSystemPath(StaticValueProvider.of(""))
            .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} passes through data when null strings as args.
   * when hasInvocable returns false.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughNullStrings() throws Exception {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson = pipeline
        .apply("Create", Create.of(inJson))
        .apply(TransformTextViaJavascript.newBuilder()
            .setFunctionName(StaticValueProvider.of(null))
            .setFileSystemPath(StaticValueProvider.of(null))
            .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} passes through data when null ValueProvider as args.
   * when hasInvocable returns false.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnPassthroughNullValueProvider() throws Exception {
    List<String> inJson = Arrays.asList("{\"answerToLife\":    42}");

    PCollection<String> transformedJson = pipeline
        .apply("Create", Create.of(inJson))
        .apply(TransformTextViaJavascript.newBuilder()
            .setFunctionName(null)
            .setFileSystemPath(null)
            .build());

    PAssert.that(transformedJson).containsInAnyOrder(inJson);

    pipeline.run();
  }

  /**
   * Test {@link TransformTextViaJavascript} allows for script to disregard some data
   * by returning null / undefined.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testDoFnDisregards() throws Exception {
    List<String> inJson = Arrays.asList(
        "{\"answerToLife\":42}", "{\"answerToLife\":43}");
    List<String> expectedJson = Arrays.asList("{\"answerToLife\":42}");

    PCollection<String> transformedJson = pipeline
        .apply("Create", Create.of(inJson))
        .apply(TransformTextViaJavascript.newBuilder()
            .setFileSystemPath(
                StaticValueProvider.of(RESOURCES + "GoodScripts/returnsUndefined.js"))
            .setFunctionName(StaticValueProvider.of("transform"))
            .build());

    PAssert.that(transformedJson).containsInAnyOrder(expectedJson);

    pipeline.run();
  }
}
