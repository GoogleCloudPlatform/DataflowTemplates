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

import com.google.ads.googleads.v14.errors.GoogleAdsError;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.Message;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.io.googleads.GoogleAdsV14.RateLimitPolicy;
import org.apache.beam.sdk.io.googleads.GoogleAdsV14.RateLimitPolicyFactory;

public class GoogleAdsRateLimitPolicyFactory implements RateLimitPolicyFactory {
  @VisibleForTesting
  static final ConcurrentMap<Double, RateLimitPolicy> CACHE = new ConcurrentHashMap<>();

  private final double permitsPerSecond;

  public GoogleAdsRateLimitPolicyFactory(double permitsPerSecond) {
    this.permitsPerSecond = permitsPerSecond;
  }

  @Override
  public RateLimitPolicy getRateLimitPolicy() {
    return CACHE.computeIfAbsent(
        permitsPerSecond,
        k ->
            new RateLimitPolicy() {
              private final RateLimiter rateLimiter = RateLimiter.create(k);

              @Override
              public void onBeforeRequest(String developerToken, String customerId, Message request)
                  throws InterruptedException {
                rateLimiter.acquire();
              }

              @Override
              public void onSuccess(String developerToken, String customerId, Message request) {}

              @Override
              public void onError(
                  String developerToken,
                  String customerId,
                  Message request,
                  GoogleAdsError error) {}
            });
  }
}
