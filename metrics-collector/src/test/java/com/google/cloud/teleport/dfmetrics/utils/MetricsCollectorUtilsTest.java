/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.dfmetrics.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetricsCollectorUtilsTest {
  @Test
  public void sanitizeJobNameWithUpperAndLowerCase() {
    // Arrange
    String prefix = "myJob";
    String expectedPrefix = "my-job";

    // Act
    String actualJobName = MetricsCollectorUtils.sanitizeJobName(prefix);

    // Assert
    assertThat(actualJobName.startsWith(expectedPrefix), equalTo(true));
  }

  @Test
  public void sanitizeJobNameWithAllUpperCase() {
    // Arrange
    String prefix = "MYJOB";
    String expectedPrefix = "m-y-j-o-b";

    // Act
    String actualJobName = MetricsCollectorUtils.sanitizeJobName(prefix);

    // Assert
    assertThat(actualJobName.startsWith(expectedPrefix), equalTo(true));
  }

  @Test
  public void classConversionTemplateEnvironment_withNumberValue_returnSuccess() {
    // Arrange
    Map<String, Object> input = new HashMap<>();
    input.put("numWorkers", 2.0);
    input.put("maxWorkers", 10.0);
    Map<String, Object> expected = new HashMap<>();
    expected.put("numWorkers", 2);
    expected.put("maxWorkers", 10);

    Map<String, Object> actual = MetricsCollectorUtils.castValuesToAppropriateTypes(input);

    // Assert
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void classConversionTemplateEnvironment_withmixedValues_returnsCorrectConversions() {
    Map<String, Object> input = new HashMap<>();
    input.put("numWorkers", 2.0);
    input.put("maxWorkers", 10.0);
    input.put("primeenabled", true);
    input.put("service_account", "test@testproject.com");

    Map<String, Object> expected = new HashMap<>();
    expected.put("numWorkers", 2);
    expected.put("maxWorkers", 10);
    expected.put("primeenabled", true);
    expected.put("service_account", "test@testproject.com");

    // Act
    Map<String, Object> actual = MetricsCollectorUtils.castValuesToAppropriateTypes(input);
    // Assert
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void classConversionTemplateEnvironment_withNumberAndStringValue_returnSuccess() {
    // Arrange
    Map<String, Object> input = new HashMap<>();
    input.put("numWorkers", 2.0);
    input.put("maxWorkers", 10.0);
    input.put("templateType", "flex");
    Map<String, Object> expected = new HashMap<>();
    expected.put("numWorkers", 2);
    expected.put("maxWorkers", 10);
    expected.put("templateType", "flex");

    Map<String, Object> actual = MetricsCollectorUtils.castValuesToAppropriateTypes(input);

    // Assert
    assertThat(actual, equalTo(expected));
  }
}
