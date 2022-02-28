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

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.cloud.teleport.v2.options.GoogleAdsOptions;
import java.io.File;
import java.io.IOException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DefaultGoogleAdsClientFactory implements GoogleAdsClientFactory {
  private static final DefaultGoogleAdsClientFactory INSTANCE = new DefaultGoogleAdsClientFactory();

  public static DefaultGoogleAdsClientFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public GoogleAdsClient newGoogleAdsClient(
      @Nullable String developerToken,
      @Nullable Long linkedCustomerId,
      @Nullable Long loginCustomerId,
      GoogleAdsOptions options) {
    GoogleAdsClient.Builder builder = GoogleAdsClient.newBuilder();

    if (options.getGoogleAdsConfigPath() != null) {
      try {
        builder.fromPropertiesFile(new File(options.getGoogleAdsConfigPath()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      builder.setEndpoint(options.getGoogleAdsEndpoint());
      builder.setCredentials(options.getGoogleAdsCredential());
    }

    if (developerToken != null) {
      builder.setDeveloperToken(developerToken);
    }

    if (linkedCustomerId != null) {
      builder.setLinkedCustomerId(linkedCustomerId);
    }

    if (loginCustomerId != null) {
      builder.setLoginCustomerId(loginCustomerId);
    }

    return builder.build();
  }
}
