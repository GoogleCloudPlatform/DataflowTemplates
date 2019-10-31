/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.templates.FileFormatConversion.FileFormatConversionOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link FileFormatConversion} class. */
public class FileFormatConversionTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static final String AVRO = "AVRO";

  /**
   * Tests {@link FileFormatConversion#run(FileFormatConversionOptions)} throws an exception if an
   * invalid file format is provided.
   */
  @Test
  public void testInvalidFileFormat() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Provide correct input/output file format.");

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    options.setInputFileFormat("INVALID");
    options.setOutputFileFormat(AVRO);

    FileFormatConversion.run(options);
  }

  /**
   * Tests {@link FileFormatConversion#run(FileFormatConversionOptions)} throws an exception if the
   * same input and output file formats are provided.
   */
  @Test
  public void testSameInputAndOutputFileFormat() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Provide correct input/output file format.");

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    options.setInputFileFormat(AVRO);
    options.setOutputFileFormat(AVRO);

    FileFormatConversion.run(options);
  }
}
