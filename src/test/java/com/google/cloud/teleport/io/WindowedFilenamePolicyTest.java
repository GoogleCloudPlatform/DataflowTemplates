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



package com.google.cloud.teleport.io;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.WindowedContext;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for {@link WindowedFilenamePolicy}.
 */
@RunWith(JUnit4.class)
public class WindowedFilenamePolicyTest {

  /**
   * Rule for exception testing.
   */
  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * The temporary folder.
   */
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * The name of the temp directory.
   */
  private static final String TEMP_DIRECTORY_NAME = "temp";

  /**
   * Gets the temporary folder resource.
   */
  private ResourceId getTemporaryFolder() {
    return LocalResources.fromFile(tmpFolder.getRoot(), /* isDirectory */ true);
  }

  /**
   * Gets the base directory for temporary files.
   */
  private ResourceId getBaseTempDirectory() {
    return getTemporaryFolder()
        .resolve(TEMP_DIRECTORY_NAME, StandardResolveOptions.RESOLVE_DIRECTORY);
  }


  /**
   * Tests that windowedFilename() constructs the filename
   * correctly according to the parameters when using ValueProviders.
   */
  @Test
  public void testWindowedFilenameFormatValueProvider() {
    // Arrange
    //
    ResourceId outputDirectory = getBaseTempDirectory();
    WindowedContext context = mock(WindowedContext.class);
    BoundedWindow window = mock(BoundedWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    String extension = "";

    when(context.getShardNumber()).thenReturn(1);
    when(context.getNumShards()).thenReturn(1);
    when(context.getWindow()).thenReturn(window);
    when(window.maxTimestamp()).thenReturn(
        new DateTime(2017, 1, 8, 10, 56, 45).toInstant());
    when(context.getPaneInfo()).thenReturn(paneInfo);

    WindowedFilenamePolicy policy = new WindowedFilenamePolicy(
        NestedValueProvider.of(
            StaticValueProvider.of("output"),
            (SerializableFunction<String, ResourceId>) input ->
                FileBasedSink.convertToFileResourceIfPossible(input)),
        StaticValueProvider.of("-SSS-of-NNN"),
        StaticValueProvider.of(".txt"));

    // Act
    //
    ResourceId filename = policy.windowedFilename(outputDirectory, context, extension);

    // Assert
    //
    assertThat(filename, is(notNullValue()));
    assertThat(filename.getFilename(), is(equalTo("output-001-of-001.txt")));
  }

  /**
   * Tests that windowedFilename() constructs the filename
   * correctly according to the parameters when using Strings.
   */
  @Test
  public void testWindowedFilenameFormatString() {
    // Arrange
    //
    ResourceId outputDirectory = getBaseTempDirectory();
    WindowedContext context = mock(WindowedContext.class);
    BoundedWindow window = mock(BoundedWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    String extension = "";

    when(context.getShardNumber()).thenReturn(1);
    when(context.getNumShards()).thenReturn(1);
    when(context.getWindow()).thenReturn(window);
    when(window.maxTimestamp()).thenReturn(
        new DateTime(2017, 1, 8, 10, 56, 45).toInstant());
    when(context.getPaneInfo()).thenReturn(paneInfo);

    WindowedFilenamePolicy policy = new WindowedFilenamePolicy(
        FileBasedSink.convertToFileResourceIfPossible("string-output"), "-SSS-of-NNN", ".csv");

    // Act
    //
    ResourceId filename = policy.windowedFilename(outputDirectory, context, extension);

    // Assert
    //
    assertThat(filename, is(notNullValue()));
    assertThat(filename.getFilename(), is(equalTo("string-output-001-of-001.csv")));
  }

  /**
   * Tests that windowedFilename() constructs the filename
   * correctly according to the parameters when the filename
   * suffix is a null value.
   */
  @Test
  public void testWindowedFilenameNullSuffix() {
    // Arrange
    //
    ResourceId outputDirectory = getBaseTempDirectory();
    WindowedContext context = mock(WindowedContext.class);
    BoundedWindow window = mock(BoundedWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    String extension = "";

    when(context.getShardNumber()).thenReturn(1);
    when(context.getNumShards()).thenReturn(1);
    when(context.getWindow()).thenReturn(window);
    when(window.maxTimestamp()).thenReturn(
        new DateTime(2017, 1, 8, 10, 56, 45).toInstant());
    when(context.getPaneInfo()).thenReturn(paneInfo);

    WindowedFilenamePolicy policy = new WindowedFilenamePolicy(
        NestedValueProvider.of(
            StaticValueProvider.of("output"),
            (SerializableFunction<String, ResourceId>) input ->
                FileBasedSink.convertToFileResourceIfPossible(input)),
        StaticValueProvider.of("-SSS-of-NNN"),
        StaticValueProvider.of(null));

    // Act
    //
    ResourceId filename = policy.windowedFilename(outputDirectory, context, extension);

    // Assert
    //
    assertThat(filename, is(notNullValue()));
    assertThat(filename.getFilename(), is(equalTo("output-001-of-001")));
  }

  /**
   * Tests that windowedFilename() produces the correct directory when
   * the directory has dynamic date variables.
   */
  @Test
  public void testWithDynamicDirectory() {
    // Arrange
    //
    ResourceId outputDirectory = getBaseTempDirectory()
        .resolve("YYYY/MM/DD/HH", StandardResolveOptions.RESOLVE_DIRECTORY);
    WindowedContext context = mock(WindowedContext.class);
    IntervalWindow window = mock(IntervalWindow.class);
    PaneInfo paneInfo = PaneInfo.createPane(false, true, Timing.ON_TIME, 0, 0);
    String extension = "";

    Instant windowBegin = new DateTime(2017, 1, 8, 10, 55, 0).toInstant();
    Instant windowEnd = new DateTime(2017, 1, 8, 10, 56, 0).toInstant();

    when(context.getShardNumber()).thenReturn(1);
    when(context.getNumShards()).thenReturn(1);
    when(context.getPaneInfo()).thenReturn(paneInfo);
    when(context.getWindow()).thenReturn(window);
    when(window.maxTimestamp()).thenReturn(windowEnd);
    when(window.start()).thenReturn(windowBegin);
    when(window.end()).thenReturn(windowEnd);

    WindowedFilenamePolicy policy = new WindowedFilenamePolicy(
        NestedValueProvider.of(
            StaticValueProvider.of("output"),
            (SerializableFunction<String, ResourceId>) input ->
                FileBasedSink.convertToFileResourceIfPossible(input)),
        StaticValueProvider.of("-SSS-of-NNN"),
        StaticValueProvider.of(null));

    // Act
    //
    ResourceId filename = policy.windowedFilename(outputDirectory, context, extension);

    // Assert
    //
    assertThat(filename, is(notNullValue()));
    assertThat(
        filename.getCurrentDirectory().toString().endsWith("2017/01/08/10/"), is(equalTo(true)));
    assertThat(filename.getFilename(), is(equalTo("output-001-of-001")));
  }

  /**
   * Tests that unwindowedFilename() fails with an
   * {@link UnsupportedOperationException} when invoked.
   */
  @Test
  public void testUnwindowedFilenameFails() {
    // Arrange
    //
    ResourceId resourceId = null;
    Context context = null;
    String extension = ".txt";

    // Act
    //
    exception.expect(UnsupportedOperationException.class);
    WindowedFilenamePolicy policy = new WindowedFilenamePolicy(
        FileBasedSink.convertToFileResourceIfPossible("string-output"), "-SSS-of-NNN", ".csv");
    policy.unwindowedFilename(resourceId, context, extension);
  }
}
