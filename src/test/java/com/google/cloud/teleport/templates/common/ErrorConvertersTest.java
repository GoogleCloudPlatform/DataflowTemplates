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

import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorMessage;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit Tests {@link ErrorConverters}. */
@RunWith(JUnit4.class)
public class ErrorConvertersTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testErrorLogs() throws IOException {
    TupleTag<String> errorTag = new TupleTag<String>("errors"){};
    TupleTag<String> goodTag = new TupleTag<String>("good"){};

    TemporaryFolder tmpFolder = new TemporaryFolder();
    tmpFolder.create();


    pipeline
        .apply(Create.of("Hello", "World", "Colin"))
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            if (c.element().equals("Hello")) {
              c.output(c.element());
            } else {
              c.output(errorTag, c.element());
            }
          }
        }).withOutputTags(goodTag, TupleTagList.of(errorTag)))
        .apply(ErrorConverters.LogErrors.newBuilder()
            .setErrorWritePath(
                StaticValueProvider.of(tmpFolder.getRoot().getAbsolutePath() + "errors.txt"))
            .setErrorTag(errorTag)
            .build());

    pipeline.run();

    // Read in tempfile data
    File file = new File(
        tmpFolder.getRoot().getAbsolutePath() + "errors.txt-00000-of-00001");
    String fileContents = Files.toString(file, Charsets.UTF_8);
    tmpFolder.delete();

    // Get the unique expected & received lines of text
    HashSet<String> expected = new HashSet<>();
    Collections.addAll(expected, "World", "Colin");

    HashSet<String> result = new HashSet<>();
    Collections.addAll(result, fileContents.split("\n"));

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testErrorMessageSerialization() throws Exception {
    ErrorMessage errorMessage = ErrorMessage.newBuilder()
        .setMessage("Some Message")
        .setTextElementType("myType")
        .setTextElementData("data 234, 2")
        .build();

    String json = errorMessage.toJson();
    ErrorMessage dupErrorMessage = ErrorMessage.fromJson(json);
    Assert.assertEquals(dupErrorMessage, errorMessage);
  }
}
