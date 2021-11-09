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
package com.google.api.services.dataplex.v1;

/**
 * CloudDataplex request initializer for setting properties like key and userIp.
 *
 * <p>The simplest usage is to use it to set the key parameter:
 *
 * <pre>
 * public static final GoogleClientRequestInitializer KEY_INITIALIZER =
 * new CloudDataplexRequestInitializer(KEY);
 * </pre>
 *
 * <p>There is also a constructor to set both the key and userIp parameters:
 *
 * <pre>
 * public static final GoogleClientRequestInitializer INITIALIZER =
 * new CloudDataplexRequestInitializer(KEY, USER_IP);
 * </pre>
 *
 * <p>If you want to implement custom logic, extend it like this:
 *
 * <pre>
 * public static class MyRequestInitializer extends CloudDataplexRequestInitializer {
 *
 * {@literal @}Override
 * public void initializeCloudDataplexRequest(CloudDataplexRequest{@literal <}?{@literal >} request)
 * throws IOException {
 * // custom logic
 * }
 * }
 * </pre>
 *
 * <p>Finally, to set the key and userIp parameters and insert custom logic, extend it like this:
 *
 * <pre>
 * public static class MyRequestInitializer2 extends CloudDataplexRequestInitializer {
 *
 * public MyKeyRequestInitializer() {
 * super(KEY, USER_IP);
 * }
 *
 * {@literal @}Override
 * public void initializeCloudDataplexRequest(CloudDataplexRequest{@literal <}?{@literal >} request)
 * throws IOException {
 * // custom logic
 * }
 * }
 * </pre>
 *
 * <p>Subclasses should be thread-safe.
 *
 * @since 1.12
 */
public class CloudDataplexRequestInitializer
    extends com.google.api.client.googleapis.services.json
        .CommonGoogleJsonClientRequestInitializer {

  public CloudDataplexRequestInitializer() {
    super();
  }

  /** @param key API key or {@code null} to leave it unchanged */
  public CloudDataplexRequestInitializer(String key) {
    super(key);
  }

  /**
   * @param key API key or {@code null} to leave it unchanged
   * @param userIp user IP or {@code null} to leave it unchanged
   */
  public CloudDataplexRequestInitializer(String key, String userIp) {
    super(key, userIp);
  }

  @Override
  public final void initializeJsonRequest(
      com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest<?> request)
      throws java.io.IOException {
    super.initializeJsonRequest(request);
    initializeCloudDataplexRequest((CloudDataplexRequest<?>) request);
  }

  /**
   * Initializes CloudDataplex request.
   *
   * <p>Default implementation does nothing. Called from {@link
   * #initializeJsonRequest(com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest)}.
   *
   * @throws java.io.IOException I/O exception
   */
  protected void initializeCloudDataplexRequest(CloudDataplexRequest<?> request)
      throws java.io.IOException {}
}
