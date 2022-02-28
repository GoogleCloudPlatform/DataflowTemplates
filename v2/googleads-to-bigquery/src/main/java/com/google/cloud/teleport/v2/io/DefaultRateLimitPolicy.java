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
package com.google.cloud.teleport.v2.io;

import com.google.ads.googleads.v10.errors.QuotaErrorDetails;
import com.google.ads.googleads.v10.services.SearchGoogleAdsStreamRequest;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.Sleeper;

public class DefaultRateLimitPolicy implements RateLimitPolicy {
  private final GoogleAdsQuotaErrorBackoffImpl backoff;
  private final RateLimiter rateLimiter;

  public DefaultRateLimitPolicy(int maxRetries, double maxQueriesPerSecond) {
    backoff = new GoogleAdsQuotaErrorBackoffImpl(maxRetries);
    rateLimiter = RateLimiter.create(maxQueriesPerSecond);
  }

  @Override
  public void onBeforeRequest(SearchGoogleAdsStreamRequest request) throws InterruptedException {
    rateLimiter.acquire();
  }

  @Override
  public void onSuccess() {
    backoff.reset();
  }

  @Override
  public void onQuotaError(QuotaErrorDetails quotaErrorDetails)
      throws IOException, InterruptedException {
    backoff.setRetryDelay(quotaErrorDetails.getRetryDelay());
    if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
      throw new IOException("Maximum retries exceeded");
    }
  }

  private static class GoogleAdsQuotaErrorBackoffImpl implements BackOff {
    private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
    private static final long DEFAULT_MIN_BACKOFF_MILLIS = 30_000;

    private final int maxRetries;
    private long retryDelayMillis = DEFAULT_MIN_BACKOFF_MILLIS;
    private int currentRetry = 0;

    public GoogleAdsQuotaErrorBackoffImpl(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    @Override
    public void reset() {
      currentRetry = 0;
    }

    public void setRetryDelay(Duration duration) {
      this.retryDelayMillis = Durations.toMillis(duration);
    }

    @Override
    public long nextBackOffMillis() {
      if (currentRetry >= maxRetries) {
        return BackOff.STOP;
      }

      double randomOffset =
          (Math.random() * 2 - 1) * DEFAULT_RANDOMIZATION_FACTOR * retryDelayMillis;
      currentRetry += 1;

      return Math.round(retryDelayMillis + randomOffset);
    }
  }
}
