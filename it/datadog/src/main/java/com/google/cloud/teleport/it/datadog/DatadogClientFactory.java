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

import com.datadog.Service;
import com.datadog.ServiceArgs;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/** Datadog Driver Factory class. */
class DatadogClientFactory {
  DatadogClientFactory() {}

  /**
   * Returns an HTTP client that is used to send HTTP messages to a Datadog Server instance with HEC.
   *
   * @return An HTTP client for sending HTTP messages to Datadog HEC.
   */
  CloseableHttpClient getHttpClient() {
    return HttpClientBuilder.create().build();
  }

  /**
   * Returns a Datadog Service client for sending requests to a Datadog Server instance.
   *
   * @param serviceArgs the service arguments to connect to the server with.
   * @return A Datadog service client to retrieve messages from a Datadog server instance.
   */
  Service getServiceClient(ServiceArgs serviceArgs) {
    return Service.connect(serviceArgs);
  }
}
