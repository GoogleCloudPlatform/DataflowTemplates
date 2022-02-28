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
package com.google.cloud.teleport.v2.options;

import com.google.auth.Credentials;
import com.google.cloud.teleport.v2.auth.GoogleAdsApplicationDefaultCredentialFactory;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface GoogleAdsOptions extends PipelineOptions {
  @Nullable
  String getGoogleAdsConfigPath();

  void setGoogleAdsConfigPath(String configPath);

  @Description("Google Ads endpoint.")
  @Default.String("googleads.googleapis.com:443")
  String getGoogleAdsEndpoint();

  void setGoogleAdsEndpoint(String endpoint);

  @Description("OAuth 2.0 Client ID.")
  String getGoogleAdsClientId();

  void setGoogleAdsClientId(String clientId);

  @Description("OAuth 2.0 Client Secret.")
  String getGoogleAdsClientSecret();

  void setGoogleAdsClientSecret(String clientSecret);

  @Description("OAuth 2.0 Refresh Token.")
  String getGoogleAdsRefreshToken();

  void setGoogleAdsRefreshToken(String refreshToken);

  @Description("")
  @Default.InstanceFactory(EmptyStringListFactory.class)
  List<String> getGoogleAdsCredentialImpersonationChain();

  void setGoogleAdsCredentialImpersonationChain(List<String> credentialsImpersonationChain);

  @Default.Class(GoogleAdsApplicationDefaultCredentialFactory.class)
  Class<? extends CredentialFactory> getGoogleAdsCredentialFactoryClass();

  void setGoogleAdsCredentialFactoryClass(
      Class<? extends CredentialFactory> credentialFactoryClass);

  @Default.InstanceFactory(GoogleAdsUserCredentialsFactory.class)
  Credentials getGoogleAdsCredential();

  void setGoogleAdsCredential(Credentials credential);

  class EmptyStringListFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(PipelineOptions options) {
      return ImmutableList.of();
    }
  }

  /**
   * Attempts to load the Google Ads credentials. See {@link CredentialFactory#getCredential()} for
   * more details.
   */
  class GoogleAdsUserCredentialsFactory implements DefaultValueFactory<Credentials> {
    @Override
    public Credentials create(PipelineOptions options) {
      GoogleAdsOptions googleAdsOptions = options.as(GoogleAdsOptions.class);
      try {
        CredentialFactory factory =
            InstanceBuilder.ofType(CredentialFactory.class)
                .fromClass(googleAdsOptions.getGoogleAdsCredentialFactoryClass())
                .fromFactoryMethod("fromOptions")
                .withArg(PipelineOptions.class, options)
                .build();
        return factory.getCredential();
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException("Unable to obtain credential", e);
      }
    }
  }
}
