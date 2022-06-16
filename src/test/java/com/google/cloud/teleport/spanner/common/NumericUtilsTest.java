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
package com.google.cloud.teleport.spanner.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Test coverage for {@link NumericUtils}. */
public class NumericUtilsTest {

  @Test
  public void testExactScale() {
    String numericValue = "42.42";
    byte[] encodedBytes = NumericUtils.pgStringToBytes(numericValue);
    assertThat(numericValue, equalTo(NumericUtils.pgBytesToString(encodedBytes)));

    String numericValueWithHighScale = "42.42000000000000000000000";
    byte[] encodedHighScaleBytes = NumericUtils.pgStringToBytes(numericValueWithHighScale);
    assertThat(
        numericValueWithHighScale, equalTo(NumericUtils.pgBytesToString(encodedHighScaleBytes)));

    assertThat(encodedBytes, not(equalTo(encodedHighScaleBytes)));
    assertTrue(encodedBytes.length < encodedHighScaleBytes.length);
  }
}
