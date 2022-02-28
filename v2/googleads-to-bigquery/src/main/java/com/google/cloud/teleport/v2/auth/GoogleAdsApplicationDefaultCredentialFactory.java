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
package com.google.cloud.teleport.v2.auth;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.teleport.v2.options.GoogleAdsOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.options.PipelineOptions;

public class GoogleAdsApplicationDefaultCredentialFactory implements CredentialFactory {
  private static final List<String> SCOPES =
      Arrays.asList("https://www.googleapis.com/auth/adwords");

  private GoogleAdsOptions options;

  private GoogleAdsApplicationDefaultCredentialFactory(GoogleAdsOptions options) {
    this.options = options;
  }

  public static GoogleAdsApplicationDefaultCredentialFactory fromOptions(PipelineOptions options) {
    return new GoogleAdsApplicationDefaultCredentialFactory(options.as(GoogleAdsOptions.class));
  }

  @Override
  public Credentials getCredential() {
    GoogleCredentials credentials;
    try {
      credentials = GoogleCredentials.getApplicationDefault().createScoped(SCOPES);
    } catch (IOException e) {
      return null;
    }

    List<String> chain = options.getGoogleAdsCredentialImpersonationChain();

    if (chain.size() > 0) {
      credentials =
          ImpersonatedCredentials.create(
              credentials,
              chain.get(chain.size() - 1),
              chain.subList(0, chain.size() - 1),
              SCOPES,
              0);
    }

    return credentials;
  }
}
