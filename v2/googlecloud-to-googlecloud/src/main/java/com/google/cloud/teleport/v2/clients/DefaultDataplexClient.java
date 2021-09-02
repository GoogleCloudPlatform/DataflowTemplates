/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.clients;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.dataplex.v1.CloudDataplex;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Assets;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.teleport.v2.values.EntityMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

/** Default Dataplex implementation. This is still a work in progress and may change. */
public final class DefaultDataplexClient implements DataplexClient {
  private static final String DEFAULT_ROOT_URL = "https://dataplex.googleapis.com/";
  private static final String DEFAULT_CLIENT_NAME = "DataflowTemplatesDataplexClient";

  private final CloudDataplex client;

  private DefaultDataplexClient(CloudDataplex client) {
    checkNotNull(client, "CloudDataplex instance cannot be null.");
    this.client = client;
  }

  /**
   * Returns an instance of {@link DefaultDataplexClient} that will utilize an instance of {@link
   * CloudDataplex}.
   *
   * @param client the instance to utilize
   * @return a new instance of {@link DefaultDataplexClient}
   */
  public static DefaultDataplexClient withClient(CloudDataplex client) {
    return new DefaultDataplexClient(client);
  }

  /**
   * Returns an instance of {@link DefaultDataplexClient} that will utilize the default {@link
   * CloudDataplex}.
   *
   * @return a new instance of {@link DefaultDataplexClient}
   */
  public static DefaultDataplexClient withDefaultClient() throws IOException {
    HttpTransport transport = Utils.getDefaultTransport();
    JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    HttpRequestInitializer httpInitializer =
        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault());

    CloudDataplex client =
        new CloudDataplex.Builder(transport, jsonFactory, httpInitializer)
            .setApplicationName(DEFAULT_CLIENT_NAME)
            .setRootUrl(DEFAULT_ROOT_URL)
            .build();

    return new DefaultDataplexClient(client);
  }

  @Override
  public GoogleCloudDataplexV1Asset getAsset(String assetName) throws IOException {
    return client.projects().locations().lakes().zones().assets().get(assetName).execute();
  }

  @Override
  public void updateMetadata(Assets asset, ImmutableList<EntityMetadata> metadata) {}
}
