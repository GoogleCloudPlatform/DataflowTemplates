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
package com.google.cloud.teleport.v2.io;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn.WindowedContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link WindowedFilenamePolicy}. */
@RunWith(JUnit4.class)
public class WindowedFilenamePolicyTest {

  private class TestOutputFileHints implements OutputFileHints {

    public String getMimeType() {
      return "";
    }

    public String getSuggestedFilenameSuffix() {
      return "";
    }
  }

  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  /** The temporary folder. */
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /** The name of the temp directory. */
  private static final String TEMP_DIRECTORY_NAME = "temp";

  /** Gets the temporary folder resource. */
  private ResourceId getTemporaryFolder() {
    return LocalResources.fromFile(tmpFolder.getRoot(), /* isDirectory */ true);
  }

  /** Gets the base directory for temporary files. */
  private ResourceId getBaseTempDirectory() {
    return getTemporaryFolder()
        .resolve(TEMP_DIRECTORY_NAME, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * Call {@link FileSystems#setDefaultPipelineOptions(PipelineOptions)} to register the file scheme
   * (gs) used during the tests.
   */
  @BeforeClass
  public static void setupFileSystem() {
    FileSystems.setDefaultPipelineOptions(TestPipeline.testingPipelineOptions());
  }

  /**
   * Tests that windowedFilename() constructs the filename correctly according to the parameters
   * when using Strings.
   */
  @Test
  public void testWindowedFilenameFormatString() throws IOException {
    // Arrange
    //
    ResourceId outputDirectory = getBaseTempDirectory();
    WindowedContext context = mock(WindowedContext.class);
    BoundedWindow window = mock(BoundedWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory(outputDirectory.toString())
            .withOutputFilenamePrefix("string-output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix(".csv");

    // Act
    //
    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    // Assert
    //
    assertThat(filename).isNotNull();
    assertThat(filename.getFilename()).isEqualTo("string-output-001-of-001.csv");
  }

  /**
   * Tests that windowedFilename() produces the correct directory when the directory has dynamic
   * date variables. Only "dd" and "mm" are tested as part of the GCS bucket name, as upper case
   * letters cannot be part of GCS bucket names.
   */
  @Test
  public void testWithDynamicDirectory() throws IOException {
    // Arrange
    WindowedContext context = mock(WindowedContext.class);
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    // Directory with "mm" specified
    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-mm/YYYY/MM/dd/HH:mm")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN");

    // Directory with "dd" specified
    WindowedFilenamePolicy policyWithDay =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-dd/YYYY/MM/dd/HH:mm")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN");

    // Act
    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());
    ResourceId filenameWithDay =
        policyWithDay.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    // Assert
    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-mm/2017/01/08/10:56/");
    assertThat(filenameWithDay.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-dd/2017/01/08/10:56/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  /**
   * Tests that unwindowedFilename() fails with an {@link UnsupportedOperationException} when
   * invoked.
   */
  @Test
  public void testUnwindowedFilenameFails() {
    // Act
    //
    exception.expect(UnsupportedOperationException.class);
    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-mm/YYYY/MM/DD/HH:mm")
            .withOutputFilenamePrefix("string-output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix(".csv");

    policy.unwindowedFilename(1, 1, new TestOutputFileHints());
  }

  /**
   * Tests that windowedFilename() produces the correct directory when using a custom DateTime
   * pattern.
   */
  @Test
  public void testWindowedDirectoryCustomPattern() {
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-y/y/M/D/H:m")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withYearPattern("y")
            .withMonthPattern("M")
            .withDayPattern("D")
            .withHourPattern("H")
            .withMinutePattern("m");

    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-y/2017/1/8/10:56/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  /**
   * Tests that windowedFilename() produces the correct directory when using a custom DateTime
   * String.
   */
  @Test
  public void testWindowedDirectoryCustomStringPattern() {
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-y/y/M/D/H:m")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix("")
            .withYearPattern("y")
            .withMonthPattern("M")
            .withDayPattern("D")
            .withHourPattern("H")
            .withMinutePattern("m");

    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-y/2017/1/8/10:56/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  /**
   * Tests that windowedFilename() produces the correct directory when a single {@link DateTime}
   * pattern override is passed.
   */
  @Test
  public void testWindowedDirectorySinglePattern() {
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-mmmm/recommendations/mmmm/")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix("")
            .withMinutePattern("mmmm");

    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-mmmm/recommendations/0056/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  /**
   * Tests that windowedFilename() produces the correct directory when it unintentionally contains
   * {@link DateTime} patterns.
   */
  @Test
  public void testWindowedDirectoryUnintentionalPattern() {
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket-mmmm/recommmmendations/mmmm/")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix("")
            .withMinutePattern("mmmm");

    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString())
        .isEqualTo("gs://test-bucket-mmmm/recommmmendations/0056/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  @Test
  public void testWindowedDirectoryWrappedPattern() {
    // Arrange
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory("gs://test-bucket/recommendations/{mm}/")
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix("")
            .withMinutePattern("{mm}");

    // Act
    ResourceId filename =
        policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());

    // Assert
    assertThat(filename).isNotNull();
    assertThat(filename.getCurrentDirectory().toString()).endsWith("recommendations/56/");
    assertThat(filename.getFilename()).isEqualTo("output-001-of-001");
  }

  /**
   * Tests that windowedFilename() throws a {@link RuntimeException} if an invalid DateTime pattern
   * is provided.
   */
  @Test
  public void testWindowedDirectoryInvalidPattern() {
    exception.expect(RuntimeException.class);
    final String invalidMinutePattern = "i";

    ResourceId outputDirectory =
        getBaseTempDirectory().resolve("test/path/i", StandardResolveOptions.RESOLVE_DIRECTORY);
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy =
        WindowedFilenamePolicy.writeWindowedFiles()
            .withOutputDirectory(outputDirectory.toString())
            .withOutputFilenamePrefix("output")
            .withShardTemplate("-SSS-of-NNN")
            .withSuffix(null)
            .withMinutePattern(invalidMinutePattern);
    policy.windowedFilename(1, 1, window, paneInfo, new TestOutputFileHints());
  }
}
