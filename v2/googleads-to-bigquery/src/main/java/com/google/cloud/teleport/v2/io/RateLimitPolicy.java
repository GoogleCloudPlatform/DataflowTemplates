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
import java.io.IOException;

public interface RateLimitPolicy {
  default void onBeforeRequest(SearchGoogleAdsStreamRequest request) throws InterruptedException {}

  default void onSuccess() {}

  default void onQuotaError(QuotaErrorDetails quotaErrorDetails)
      throws IOException, InterruptedException {}
}
