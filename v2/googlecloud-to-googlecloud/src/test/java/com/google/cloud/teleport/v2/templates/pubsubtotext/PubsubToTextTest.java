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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.utils.DurationUtils;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link PubsubToText}. */
@RunWith(JUnit4.class)
public class PubsubToTextTest {

  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  /** Tests parseDuration() with a valid value. */
  @Test
  public void testParseDuration() {
    String value = "2m";

    Duration duration = DurationUtils.parseDuration(value);
    assertThat(duration, is(notNullValue()));
    assertThat(duration.getStandardMinutes(), is(equalTo(2L)));
  }

  /** Tests parseDuration() when the value is null. */
  @Test
  public void testParseDurationNull() {
    String value = null;

    exception.expect(NullPointerException.class);
    exception.expectMessage("The specified duration must be a non-null value!");
    DurationUtils.parseDuration(value);
  }

  /** Tests parseDuration() when given a negative value. */
  @Test
  public void testParseDurationNegative() {
    String value = "-2m";

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("The window duration must be greater than 0!");
    DurationUtils.parseDuration(value);
  }
}
