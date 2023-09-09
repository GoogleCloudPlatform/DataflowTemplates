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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.beam.sdk.io.googleads.GoogleAdsV14.RateLimitPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// The GoogleAdsRateLimitPolicyFactory used in this template should reuse the same policy across all
// DoFn instances.
// This guarantees that there's one RateLimiter per requested QPS per JVM/worker.
// A more sophisticated policy is recommended for complex setups, see
// https://developers.google.com/google-ads/api/docs/best-practices/rate-limits.
@RunWith(JUnit4.class)
public class GoogleAdsRateLimitPolicyFactoryTest {
  @Test
  public void testFactoryReturnsCachedPolicyForEqualQps() {
    GoogleAdsRateLimitPolicyFactory factory = new GoogleAdsRateLimitPolicyFactory(3.14);
    RateLimitPolicy policy = factory.getRateLimitPolicy();

    for (int i = 0; i < 3; ++i) {
      assertSame(policy, new GoogleAdsRateLimitPolicyFactory(3.14).getRateLimitPolicy());
    }
  }

  @Test
  public void testFactoryReturnsCachedPolicyForUnequalQps() {
    GoogleAdsRateLimitPolicyFactory factory = new GoogleAdsRateLimitPolicyFactory(3.14);
    RateLimitPolicy policy = factory.getRateLimitPolicy();

    for (int i = 0; i < 3; ++i) {
      assertNotSame(policy, new GoogleAdsRateLimitPolicyFactory(3.1).getRateLimitPolicy());
    }
  }
}
