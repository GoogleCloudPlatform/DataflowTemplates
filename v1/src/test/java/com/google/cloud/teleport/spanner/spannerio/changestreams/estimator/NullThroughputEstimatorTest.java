/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio.changestreams.estimator;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import org.junit.Test;

public class NullThroughputEstimatorTest {
  private static final double DELTA = 1e-10;

  @Test
  public void alwaysReturns0AsEstimatedThroughput() {
    final NullThroughputEstimator<byte[]> estimator = new NullThroughputEstimator<>();
    assertEquals(estimator.get(), 0D, DELTA);

    estimator.update(Timestamp.ofTimeSecondsAndNanos(1, 0), new byte[10]);
    assertEquals(estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(1, 0)), 0D, DELTA);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(2, 0), new byte[20]);
    assertEquals(estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(2, 0)), 0D, DELTA);
  }
}
