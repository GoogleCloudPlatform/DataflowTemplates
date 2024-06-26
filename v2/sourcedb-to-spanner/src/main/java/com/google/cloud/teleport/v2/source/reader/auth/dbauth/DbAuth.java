/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.auth.dbauth;

import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Interface for Various ways of providing DB credentials to be provided to {@link
 * org.apache.beam.sdk.io.jdbc.JdbcIO JdbcIO}.
 */
public interface DbAuth extends Serializable {

  /**
   * Get Value provider for DB userName.
   *
   * @return the username to be provided to {@link org.apache.beam.sdk.io.jdbc.JdbcIO JdbcIO}
   */
  ValueProvider<String> getUserName();

  /**
   * Get Value provider for DB password.
   *
   * @return the password to be provided to {@link org.apache.beam.sdk.io.jdbc.JdbcIO JdbcIO}
   */
  ValueProvider<String> getPassword();
}
