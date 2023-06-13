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
package com.google.cloud.teleport.it.datadog;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockserver.client.MockServerClient;

/** Datadog Driver Factory class. */
class DatadogClientFactory {
  DatadogClientFactory() {}

  /**
   * Returns an HTTP client that is used to send HTTP messages to Datadog API.
   *
   * @return An HTTP client for sending HTTP messages to Datadog API.
   */
  CloseableHttpClient getHttpClient() {
    return HttpClientBuilder.create().disableContentCompression().build();
  }

  /**
   * Returns a {@link MockServerClient} for sending requests to a MockServer instance.
   *
   * @param host the service host.
   * @param port the service port.
   * @return A {@link MockServerClient} to retrieve messages from a MockServer instance.
   */
  MockServerClient getServiceClient(String host, int port) {
    return new MockServerClient(host, port);
  }
}
