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
package com.google.cloud.teleport.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
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

  // Exclude alphanumeric and directory delimiter from wrapping the Joda pattern.
  private static final String EXCLUDED_GROUP_WRAPPER_REGEX = "[^A-Za-z0-9/]";

  private static final String YEAR_GROUP_NAME = "year";
  private static final Pattern YEAR_PATTERN_REGEX =
      createUserPatternRegex(YEAR_GROUP_NAME, "y+", "Y+");

  private static final String MONTH_GROUP_NAME = "month";
  private static final Pattern MONTH_PATTERN_REGEX = createUserPatternRegex(MONTH_GROUP_NAME, "M+");

  private static final String DAY_GROUP_NAME = "day";
  private static final Pattern DAY_PATTERN_REGEX =
      createUserPatternRegex(DAY_GROUP_NAME, "D+", "d+");

  private static final String HOUR_GROUP_NAME = "hour";
  private static final Pattern HOUR_PATTERN_REGEX = createUserPatternRegex(HOUR_GROUP_NAME, "H+");

  private static final String MINUTE_GROUP_NAME = "minute";
  private static final Pattern MINUTE_PATTERN_REGEX =
      createUserPatternRegex(MINUTE_GROUP_NAME, "m+");

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
    checkArgument(
        outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
    return toBuilder().setOutputDirectory(outputDirectory).build();
  }

  /**
   * Sets the filename prefix of the files to write to.
   *
   * @param outputFilenamePrefix The prefix of the file to output.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputFilenamePrefix(
      ValueProvider<String> outputFilenamePrefix) {
    checkArgument(
        outputFilenamePrefix != null,
        "withOutputFilenamePrefix(outputFilenamePrefix) called with null input.");
    return toBuilder().setOutputFilenamePrefix(outputFilenamePrefix).build();
  }

  /**
   * Sets the shard template of the output file.
   *
   * @param shardTemplate The shard template used during file formatting.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withShardTemplate(ValueProvider<String> shardTemplate) {
    checkArgument(
        shardTemplate != null, "withShardTemplate(shardTemplate) called with null input.");
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
   * Same as {@link WindowedFilenamePolicy#withOutputDirectory(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param outputDirectory The filename baseFile.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputDirectory(String outputDirectory) {
    checkArgument(
        outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
    return toBuilder().setOutputDirectory(StaticValueProvider.of(outputDirectory)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withOutputFilenamePrefix(ValueProvider)} but without
   * {@link ValueProvider}.
   *
   * @param outputFilenamePrefix The prefix of the file to output.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withOutputFilenamePrefix(String outputFilenamePrefix) {
    checkArgument(
        outputFilenamePrefix != null,
        "withOutputFilenamePrefix(outputFilenamePrefix) called with null input.");
    return toBuilder()
        .setOutputFilenamePrefix(StaticValueProvider.of(outputFilenamePrefix))
        .build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withShardTemplate(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param shardTemplate The shard template used during file formatting.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withShardTemplate(String shardTemplate) {
    checkArgument(
        shardTemplate != null, "withShardTemplate(shardTemplate) called with null input.");
    return toBuilder().setShardTemplate(StaticValueProvider.of(shardTemplate)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withSuffix(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param suffix The filename suffix.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withSuffix(String suffix) {
    checkArgument(suffix != null, "withSuffix(suffix) called with null input.");
    return toBuilder().setSuffix(StaticValueProvider.of(suffix)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withYearPattern(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param year The custom year pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withYearPattern(String year) {
    checkArgument(year != null, "withYear(year) called with null input.");
    return toBuilder().setYearPattern(StaticValueProvider.of(year)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withMonthPattern(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param month The custom month pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withMonthPattern(String month) {
    checkArgument(month != null, "withMonth(month) called with null input.");
    return toBuilder().setMonthPattern(StaticValueProvider.of(month)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withDayPattern(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param day The custom day pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withDayPattern(String day) {
    checkArgument(day != null, "withDay(day) called with null input.");
    return toBuilder().setDayPattern(StaticValueProvider.of(day)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withHourPattern(ValueProvider)} but without {@link
   * ValueProvider}.
   *
   * @param hour The custom hour pattern.
   * @return {@link WindowedFilenamePolicy}
   */
  public WindowedFilenamePolicy withHourPattern(String hour) {
    checkArgument(hour != null, "withHour(hour) called with null input.");
    return toBuilder().setHourPattern(StaticValueProvider.of(hour)).build();
  }

  /**
   * Same as {@link WindowedFilenamePolicy#withMinutePattern(ValueProvider)} but without {@link
   * ValueProvider}.
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
   * dynamically changing of the output location based on the window end time. A custom pattern can
   * be used, if provided.
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

      String userYearPattern = getUserPattern(yearPattern(), /* defaultValue= */ "YYYY");
      String userMonthPattern = getUserPattern(monthPattern(), /* defaultValue= */ "MM");
      String userDayPattern = getUserPattern(dayPattern(), /* defaultValue= */ "dd");
      String userHourPattern = getUserPattern(hourPattern(), /* defaultValue= */ "HH");
      String userMinutePattern = getUserPattern(minutePattern(), /* defaultValue= */ "mm");
      LOG.debug(
          "User patterns set to: Year: {}, Month: {}, Day: {}, Hour: {}, Minute: {}",
          userDayPattern,
          userMonthPattern,
          userDayPattern,
          userHourPattern,
          userMinutePattern);

      String jodaYearPattern = getJodaPattern(userYearPattern, YEAR_PATTERN_REGEX, YEAR_GROUP_NAME);
      String jodaMonthPattern =
          getJodaPattern(userMonthPattern, MONTH_PATTERN_REGEX, MONTH_GROUP_NAME);
      String jodaDayPattern = getJodaPattern(userDayPattern, DAY_PATTERN_REGEX, DAY_GROUP_NAME);
      String jodaHourPattern = getJodaPattern(userHourPattern, HOUR_PATTERN_REGEX, HOUR_GROUP_NAME);
      String jodaMinutePattern =
          getJodaPattern(userMinutePattern, MINUTE_PATTERN_REGEX, MINUTE_GROUP_NAME);
      LOG.debug(
          "Time patterns set to: Year: {}, Month: {}, Day: {}, Hour: {}, Minute: {}",
          jodaYearPattern,
          jodaMonthPattern,
          jodaDayPattern,
          jodaHourPattern,
          jodaMinutePattern);

      try {
        final DateTimeFormatter yearFormatter = DateTimeFormat.forPattern(jodaYearPattern);
        final DateTimeFormatter monthFormatter = DateTimeFormat.forPattern(jodaMonthPattern);
        final DateTimeFormatter dayFormatter = DateTimeFormat.forPattern(jodaDayPattern);
        final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(jodaHourPattern);
        final DateTimeFormatter minuteFormatter = DateTimeFormat.forPattern(jodaMinutePattern);

        outputPath = outputPath.replace(userYearPattern, yearFormatter.print(time));
        outputPath = outputPath.replace(userMonthPattern, monthFormatter.print(time));
        outputPath = outputPath.replace(userDayPattern, dayFormatter.print(time));
        outputPath = outputPath.replace(userHourPattern, hourFormatter.print(time));
        outputPath = outputPath.replace(userMinutePattern, minuteFormatter.print(time));
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(
            "Could not resolve custom DateTime pattern. " + e.getMessage(), e);
      }

      outputDirectory = FileSystems.matchNewResource(outputPath, true);
    }
    return outputDirectory;
  }

  /**
   * Returns a compiled {@link Pattern} whose regex will include a group with {@code
   * timePatternGroupName} with all the options in {@code timePatterns}.
   *
   * <p>The resulting group will be equivalent to the following in RE2 syntax:
   * (?P<timePatternGroupName>timePattern1|...|timePatternN)
   *
   * <p>The above group will also be surrounded by an optional match of {@link
   * WindowedFilenamePolicy#EXCLUDED_GROUP_WRAPPER_REGEX}.
   */
  private static Pattern createUserPatternRegex(
      String timePatternGroupName, String... timePatterns) {
    String timePatternGroup = Joiner.on('|').join(timePatterns);
    String regex =
        String.format(
            "%s?(?P<%s>%s)%s?",
            EXCLUDED_GROUP_WRAPPER_REGEX,
            timePatternGroupName,
            timePatternGroup,
            EXCLUDED_GROUP_WRAPPER_REGEX);
    return Pattern.compile(regex);
  }

  /**
   * Returns the {@code userValue} string value or {@code default} if the user value isn't provided.
   */
  private static String getUserPattern(
      @Nullable ValueProvider<String> userValue, String defaultValue) {
    String userPattern =
        userValue != null && !Strings.isNullOrEmpty(userValue.get())
            ? userValue.get()
            : defaultValue;
    LOG.debug("User pattern set to: {}", userPattern);
    return userPattern;
  }

  /**
   * Gets the Joda-compliant pattern from the user-provided pattern.
   *
   * @param userPattern user-provided pattern for replacement
   * @param regex regex that defines valid user patterns
   * @param groupName named group within regex that matches the Joda-compliant pattern
   * @return the Joda-compliant pattern (equivalent to {@code groupName})
   */
  private static String getJodaPattern(String userPattern, Pattern regex, String groupName) {
    Matcher matcher = regex.matcher(userPattern);
    checkArgument(
        matcher.find(), "Datetime pattern '%s' does not match regex '%s'", userPattern, regex);
    return matcher.group(groupName);
  }
}
