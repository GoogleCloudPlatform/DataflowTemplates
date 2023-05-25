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
package com.google.cloud.teleport.it.common.utils;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class MetricsConfigurationTest {

  @Test
  public void testCalculateAverageFilterBeginEndHalfAve() {
    List<Double> testCase = ImmutableList.of(0., 1., 10., 100., 101., 102., 103., 50., 20., 20.);
    assertEquals(50.7, MetricsConfiguration.calculateAverage(testCase), 1e-10);
    assertEquals(
        101.5,
        MetricsConfiguration.calculateAverage(
            testCase, MetricsConfiguration.filterBeginEndHalfAveFn()),
        1e-10);
  }

  @Test
  public void testCalculateAverageFilterBeginEndHalfAveShort() {
    List<Double> testCase0 = ImmutableList.of();
    assertEquals(
        Double.valueOf(0.),
        MetricsConfiguration.calculateAverage(
            testCase0, MetricsConfiguration.filterBeginEndHalfAveFn()));
    List<Double> testCase1 = ImmutableList.of(1.);
    assertEquals(
        Double.valueOf(1.),
        MetricsConfiguration.calculateAverage(
            testCase1, MetricsConfiguration.filterBeginEndHalfAveFn()));
    List<Double> testCase2 = ImmutableList.of(1., 100.);
    assertEquals(
        Double.valueOf(100.),
        MetricsConfiguration.calculateAverage(
            testCase2, MetricsConfiguration.filterBeginEndHalfAveFn()));
  }
}
