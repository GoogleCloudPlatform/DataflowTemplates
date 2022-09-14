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
package com.google.cloud.teleport.v2.templates.pubsubtotext;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.joda.time.format.DateTimeFormat;

/**
 * Provides options for shards and overriding {@link DateTimeFormat} patterns for windowed files.
 */
public interface WindowedFilenamePolicyOptions extends PipelineOptions {

  @Description(
      "The shard template of the output file. Specified as repeating sequences "
          + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
          + "shard number, or number of shards respectively")
  @Default.String("W-P-SS-of-NN")
  ValueProvider<String> getOutputShardTemplate();

  void setOutputShardTemplate(ValueProvider<String> value);

  @Description("The maximum number of output shards produced when writing.")
  @Default.Integer(1)
  Integer getNumShards();

  void setNumShards(Integer value);

  @Description(
      "The window duration in which data will be written. Defaults to 5m. "
          + "Allowed formats are: "
          + "Ns (for seconds, example: 5s), "
          + "Nm (for minutes, example: 12m), "
          + "Nh (for hours, example: 2h).")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String value);

  @Description("The custom year pattern to use for the output directory.")
  ValueProvider<String> getYearPattern();

  void setYearPattern(ValueProvider<String> yearPattern);

  @Description("The custom month pattern to use for the output directory.")
  ValueProvider<String> getMonthPattern();

  void setMonthPattern(ValueProvider<String> monthPattern);

  @Description("The custom day pattern to use for the output directory.")
  ValueProvider<String> getDayPattern();

  void setDayPattern(ValueProvider<String> dayPattern);

  @Description("The custom hour pattern to use for the output directory.")
  ValueProvider<String> getHourPattern();

  void setHourPattern(ValueProvider<String> hourPattern);

  @Description("The custom minute pattern to use for the output directory.")
  ValueProvider<String> getMinutePattern();

  void setMinutePattern(ValueProvider<String> minutePattern);
}
