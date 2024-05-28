/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.metadata.util;

import static com.google.cloud.teleport.metadata.util.EnumBasedExperimentValueProvider.STREAMING_MODE_ENUM_BASED_EXPERIMENT_VALUE_PROVIDER;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.metadata.Template;
import org.junit.Test;

/** Tests for {@link EnumBasedExperimentValueProvider}. */
public class EnumBasedExperimentValueProviderTest {
  @Test
  public void givenValidValue_converts() {
    assertThat(
            STREAMING_MODE_ENUM_BASED_EXPERIMENT_VALUE_PROVIDER.convert(
                Template.StreamingMode.AT_LEAST_ONCE))
        .isEqualTo("streaming_mode_at_least_once");
    assertThat(
            STREAMING_MODE_ENUM_BASED_EXPERIMENT_VALUE_PROVIDER.convert(
                Template.StreamingMode.EXACTLY_ONCE))
        .isEqualTo("streaming_mode_exactly_once");
  }

  @Test
  public void givenInvalidValue_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            STREAMING_MODE_ENUM_BASED_EXPERIMENT_VALUE_PROVIDER.convert(
                Template.StreamingMode.UNSPECIFIED));
  }
}
