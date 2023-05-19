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
package com.google.cloud.teleport.it.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/** Splunk Driver Factory class. */
class SplunkClientFactory {
  SplunkClientFactory() {}

  /**
   * Returns an HTTP client that is used to send HTTP messages to a Splunk Server instance with HEC.
   *
   * @return An HTTP client for sending HTTP messages to Splunk HEC.
   */
  CloseableHttpClient getHttpClient() {
    return HttpClientBuilder.create().build();
  }

  /**
   * Returns a Splunk Service client for sending requests to a Splunk Server instance.
   *
   * @param serviceArgs the service arguments to connect to the server with.
   * @return A Splunk service client to retrieve messages from a Splunk server instance.
   */
  Service getServiceClient(ServiceArgs serviceArgs) {
    return Service.connect(serviceArgs);
  }
}
