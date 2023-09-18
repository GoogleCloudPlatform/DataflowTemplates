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
package com.google.cloud.teleport.dfmetrics.model;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import java.io.IOException;

/** Class {@link DataflowClient} encapsulates the client instance to interact with Dataflow API. */
@AutoValue
public class DataflowClient {
  private static final String APPLICATION_NAME = "google-teleport/metrics-collector/1.0";

  public static Builder builder() {
    return new AutoValue_DataflowClient.Builder();
  }

  /** Builder for {@link DataflowClient}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public Dataflow build() {
      Credentials credentials = getCredentials() != null ? getCredentials() : googleCredentials();
      return new Dataflow.Builder(
              Utils.getDefaultTransport(),
              Utils.getDefaultJsonFactory(),
              new HttpCredentialsAdapter(credentials))
          .setApplicationName(APPLICATION_NAME)
          .build();
    }

    private Credentials googleCredentials() {
      try {
        return GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to get credentials! \n"
                + "Please run the `gcloud auth application-default login` command and set following environment variable: \n"
                + "\t export GOOGLE_APPLICATION_CREDENTIALS=/Users/${USER}/.config/gcloud/application_default_credentials.json");
      }
    }
  }
}
