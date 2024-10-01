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
package com.google.cloud.teleport.v2.kafka.values;

public class KafkaAuthenticationMethod {
  // Note: this is not implemented as an Enum, because these constants are used
  // in @Template.Parameter annotations, and annotations can only have constant attributes.

  public static final String NONE = "NONE";
  public static final String TLS = "TLS";
  public static final String SASL_PLAIN = "SASL_PLAIN";
  public static final String APPLICATION_DEFAULT_CREDENTIALS = "APPLICATION_DEFAULT_CREDENTIALS";
  public static final String OAUTH = "OAUTH";
}
