/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.failureinjection;

import java.io.Serializable;

/** Policy that never injects errors. */
public class NoOpPolicy implements ErrorInjectionPolicy, Serializable {
  public NoOpPolicy() {}

  @Override
  public boolean shouldInjectionError() {
    return false;
  }

  @Override
  public String toString() {
    return "NoOpPolicy";
  }
}
