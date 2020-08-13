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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WindowedFilenamePolicy} class outputs filenames for file sinks which handle windowed
 * writes.
 */
@SuppressWarnings("serial")
@AutoValue
public abstract class WindowedFilenamePolicy extends FilenamePolicy {
  /** The logger to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(WindowedFilenamePolicy.class);
  
  public static WindowedFilenamePolicy writeWindowedFiles() {
    return new AutoValue_WindowedFilenamePolicy.Builder().build();
  }
  
  @Nullable
  abstract ValueProvider<String> outputDirectory();
  
  @Nullable
  abstract ValueProvider<String> outputFilenamePrefix();
  
  @Nullable
  abstract ValueProvider<String> shardTemplate();
  
  @Nullable
  abstract ValueProvider<String> suffix();
  
  @Nullable
  abstract ValueProvider<String> yearPattern();
  
  @Nullable
  abstract ValueProvider<String> monthPattern();
  
  @Nullable
  abstract ValueProvider<String> dayPattern();
  
  @Nullable
  abstract ValueProvider<String> hourPattern();
  
  @Nullable
  abstract ValueProvider<String> minutePattern();
  
  abstract Builder toBuilder();
  
  @AutoValue.Builder
  abstract static class Builder {
    
    abstract Builder setOutputDirectory(ValueProvider<String> outputDirectory);
    
    abstract Builder setOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix);
    
    abstract Builder setShardTemplate(ValueProvider<String> shardTemplate);
    
    abstract Builder setSuffix(ValueProvider<String> suffix);
    
    abstract Builder setYearPattern(ValueProvider<String> year);
    
    abstract Builder setMonthPattern(ValueProvider<String> month);
    
    abstract Builder setDayPattern(ValueProvider<String> day);
    
    abstract Builder setHourPattern(ValueProvider<String> hour);
    
    abstract Builder setMinutePattern(ValueProvider<String> minute);
    
    abstract WindowedFilenamePolicy build();
  }
  
  /**
   * Sets the directory to output files to.
   *
   * @param outputDirectory The filename baseFile.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputDirectory(ValueProvider<String> outputDirectory) {
    checkArgument(outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
    return toBuilder().setOutputDirectory(outputDirectory).build();
  }
  
  /**
   * Sets the filename prefix of the files to write to.
   *
   * @param outputFilenamePrefix The prefix of the file to output.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix) {
    checkArgument(outputFilenamePrefix != null, "withOutputFilenamePrefix(outputFilenamePrefix) called with null input.");
    return toBuilder().setOutputFilenamePrefix(outputFilenamePrefix).build();
  }
  
  /**
   * Sets the shard template of the output file.
   *
   * @param shardTemplate The shard template used during file formatting.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withShardTemplate(ValueProvider<String> shardTemplate) {
    checkArgument(shardTemplate != null, "withShardTemplate(shardTemplate) called with null input.");
    return toBuilder().setShardTemplate(shardTemplate).build();
  }
  
  /**
   * Sets the suffix of the files to write.
   *
   * @param suffix The filename suffix.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withSuffix(ValueProvider<String> suffix) {
    checkArgument(suffix != null, "withSuffix(suffix) called with null input.");
    return toBuilder().setSuffix(suffix).build();
  }
  
  /**
   * Sets the custom year pattern to use for the output directory.
   *
   * @param year The custom year pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withYearPattern(ValueProvider<String> year) {
    checkArgument(year != null, "withYear(year) called with null input.");
    return toBuilder().setYearPattern(year).build();
  }
  
  /**
   * Sets the custom month pattern to use for the output directory.
   *
   * @param month The custom month pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withMonthPattern(ValueProvider<String> month) {
    checkArgument(month != null, "withMonth(month) called with null input.");
    return toBuilder().setMonthPattern(month).build();
  }
  
  /**
   * Sets the custom day pattern to use for the output directory.
   *
   * @param day The custom day pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withDayPattern(ValueProvider<String> day) {
    checkArgument(day != null, "withDay(day) called with null input.");
    return toBuilder().setDayPattern(day).build();
  }
  
  /**
   * Sets the custom hour pattern to use for the output directory.
   *
   * @param hour The custom hour pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withHourPattern(ValueProvider<String> hour) {
    checkArgument(hour != null, "withHour(hour) called with null input.");
    return toBuilder().setHourPattern(hour).build();
  }
  
  /**
   * Sets the custom minute pattern to use for the output directory.
   *
   * @param minute The custom minute pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withMinutePattern(ValueProvider<String> minute) {
    checkArgument(minute != null, "withMinute(minute) called with null input.");
    return toBuilder().setMinutePattern(minute).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withOutputDirectory(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param outputDirectory The filename baseFile.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputDirectory(String outputDirectory) {
    checkArgument(outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
    return toBuilder().setOutputDirectory(StaticValueProvider.of(outputDirectory)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withOutputFilenamePrefix(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param outputFilenamePrefix The prefix of the file to output.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputFilenamePrefix(String outputFilenamePrefix) {
    checkArgument(outputFilenamePrefix != null, "withOutputFilenamePrefix(outputFilenamePrefix) called with null input.");
    return toBuilder().setOutputFilenamePrefix(StaticValueProvider.of(outputFilenamePrefix)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withShardTemplate(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param shardTemplate The shard template used during file formatting.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withShardTemplate(String shardTemplate) {
    checkArgument(shardTemplate != null, "withShardTemplate(shardTemplate) called with null input.");
    return toBuilder().setShardTemplate(StaticValueProvider.of(shardTemplate)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withSuffix(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param suffix The filename suffix.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withSuffix(String suffix) {
    checkArgument(suffix != null, "withSuffix(suffix) called with null input.");
    return toBuilder().setSuffix(StaticValueProvider.of(suffix)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withYearPattern(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param year The custom year pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withYearPattern(String year) {
    checkArgument(year != null, "withYear(year) called with null input.");
    return toBuilder().setYearPattern(StaticValueProvider.of(year)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withMonthPattern(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param month The custom month pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withMonthPattern(String month) {
    checkArgument(month != null, "withMonth(month) called with null input.");
    return toBuilder().setMonthPattern(StaticValueProvider.of(month)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withDayPattern(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param day The custom day pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withDayPattern(String day) {
    checkArgument(day != null, "withDay(day) called with null input.");
    return toBuilder().setDayPattern(StaticValueProvider.of(day)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withHourPattern(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param hour The custom hour pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withHourPattern(String hour) {
    checkArgument(hour != null, "withHour(hour) called with null input.");
    return toBuilder().setHourPattern(StaticValueProvider.of(hour)).build();
  }
  
  /**
   * Same as {@link WindowedFilenamePolicy#withMinutePattern(ValueProvider)} but without {@link ValueProvider}.
   *
   * @param minute The custom minute pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withMinutePattern(String minute) {
    checkArgument(minute != null, "withMinute(minute) called with null input.");
    return toBuilder().setMinutePattern(StaticValueProvider.of(minute)).build();
  }

  /**
   * The windowed filename method will construct filenames per window according to the baseFile,
   * suffix, and shardTemplate supplied. Directories with date templates in them will automatically
   * have their values resolved. For example the outputDirectory of /YYYY/MM/DD would resolve to
   * /2017/01/08 on January 8th, 2017.
   */
  @Override
  public ResourceId windowedFilename(
      int shardNumber,
      int numShards,
      BoundedWindow window,
      PaneInfo paneInfo,
      OutputFileHints outputFileHints) {

    checkArgument(
        outputDirectory() != null && outputDirectory().get() != null,
        "Output directory is required for writing files.");

    String shardTemplate = "";
    String suffix = "";
    String outputFilenamePrefix = "";

    if (shardTemplate() != null) {
      if (shardTemplate().get() != null) {
        shardTemplate = shardTemplate().get();
      }
    }
    if (suffix() != null) {
      if (suffix().get() != null) {
        suffix = suffix().get();
      }
    }
    if (outputFilenamePrefix() != null) {
      if (outputFilenamePrefix().get() != null) {
        outputFilenamePrefix = outputFilenamePrefix().get();
      }
    }

    ResourceId outputFile =
        resolveWithDateTemplates(outputDirectory(), window)
            .resolve(outputFilenamePrefix, StandardResolveOptions.RESOLVE_FILE);

    DefaultFilenamePolicy policy =
        DefaultFilenamePolicy.fromStandardParameters(
            StaticValueProvider.of(outputFile), shardTemplate, suffix, true);
    ResourceId result =
        policy.windowedFilename(shardNumber, numShards, window, paneInfo, outputFileHints);
    LOG.debug("Windowed file name policy created: {}", result.toString());
    return result;
  }

  /**
   * Unwindowed writes are unsupported by this filename policy so an {@link
   * UnsupportedOperationException} will be thrown if invoked.
   */
  @Override
  public ResourceId unwindowedFilename(
      int shardNumber, int numShards, OutputFileHints outputFileHints) {
    throw new UnsupportedOperationException(
        "There is no windowed filename policy for "
            + "unwindowed file output. Please use the WindowedFilenamePolicy with windowed "
            + "writes or switch filename policies.");
  }

  /**
   * Resolves any date variables which exist in the output directory path. This allows for the
   * dynamically changing of the output location based on the window end time.
   * A custom pattern can be used, if provided.
   *
   * @return The new output directory with all variables resolved.
   */
  public ResourceId resolveWithDateTemplates(
      ValueProvider<String> outputDirectoryStr, BoundedWindow window) {
    ResourceId outputDirectory = FileSystems.matchNewResource(outputDirectoryStr.get(), true);
  
    if (window instanceof IntervalWindow) {
      IntervalWindow intervalWindow = (IntervalWindow) window;
      DateTime time = intervalWindow.end().toDateTime();
      String outputPath = outputDirectory.toString();

      // Default Date patterns.
      String yearPattern = "YYYY";
      String monthPattern = "MM";
      String dayPattern = "DD";
      String hourPattern = "HH";
      String minutePattern = "mm";

      // Use a custom pattern, if provided.
      if (yearPattern() != null) {
        yearPattern = MoreObjects.firstNonNull(yearPattern().get(), yearPattern);
      }
      LOG.debug("Year pattern set to: {}", yearPattern);

      if (monthPattern() != null) {
        monthPattern = MoreObjects.firstNonNull(monthPattern().get(), monthPattern);
      }
      LOG.debug("Month pattern set to: {}", monthPattern);

      if (dayPattern() != null) {
        dayPattern = MoreObjects.firstNonNull(dayPattern().get(), dayPattern);
      }
      LOG.debug("Day pattern set to: {}", dayPattern);

      if (hourPattern() != null) {
        hourPattern = MoreObjects.firstNonNull(hourPattern().get(), hourPattern);
      }
      LOG.debug("Hour pattern set to: {}", hourPattern);

      if (minutePattern() != null) {
        minutePattern = MoreObjects.firstNonNull(minutePattern().get(), minutePattern);
      }
      LOG.debug("Minute pattern set to: {}", minutePattern);

      try {
        final DateTimeFormatter yearFormatter = DateTimeFormat.forPattern(yearPattern);
        final DateTimeFormatter monthFormatter = DateTimeFormat.forPattern(monthPattern);
        final DateTimeFormatter dayFormatter = DateTimeFormat.forPattern(dayPattern);
        final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(hourPattern);
        final DateTimeFormatter minuteFormatter = DateTimeFormat.forPattern(minutePattern);
    
        outputPath = outputPath.replace(yearPattern, yearFormatter.print(time));
        outputPath = outputPath.replace(monthPattern, monthFormatter.print(time));
        outputPath = outputPath.replace(dayPattern, dayFormatter.print(time));
        outputPath = outputPath.replace(hourPattern, hourFormatter.print(time));
        outputPath = outputPath.replace(minutePattern, minuteFormatter.print(time));
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Could not resolve custom DateTime pattern. " + e.getMessage(), e);
      }
  
      outputDirectory = FileSystems.matchNewResource(outputPath, true);
    }
    return outputDirectory;
  }
}
