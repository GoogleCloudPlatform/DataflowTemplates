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
package com.google.cloud.teleport.it.common.matchers;

import static com.google.common.truth.Truth.assertAbout;

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Assert utilities for Template DSL-like tests. */
public class TemplateAsserts {

  /**
   * Creates a {@link LaunchInfoSubject} to assert information returned from pipeline launches.
   *
   * @param launchInfo Launch information returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static LaunchInfoSubject assertThatPipeline(LaunchInfo launchInfo) {
    return assertAbout(LaunchInfoSubject.launchInfo()).that(launchInfo);
  }

  /**
   * Creates a {@link ResultSubject} to add assertions based on a pipeline result.
   *
   * @param result Pipeline result returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static ResultSubject assertThatResult(Result result) {
    return assertAbout(ResultSubject.result()).that(result);
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in a map list format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatRecords(@Nullable List<Map<String, Object>> records) {
    return assertAbout(RecordsSubject.records()).that(records);
  }
}
