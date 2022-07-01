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
package com.google.cloud.teleport.v2.clients;

import com.google.auth.Credentials;
import java.io.IOException;
import java.io.Serializable;

/** A simple factory for creating {@link DataplexClient} instances. */
public interface DataplexClientFactory extends Serializable {
  /** Creates a new instance of {@link DataplexClient}. */
  DataplexClient createClient() throws IOException;

  /**
   * Creates the default factory producing Dataplex clients that use the provided {@code credential}
   * for connection to Dataplex.
   */
  static DataplexClientFactory defaultFactory(Credentials credential) {
    return () -> DefaultDataplexClient.withDefaultClient(credential);
  }
}
