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
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.Timestamp;
import java.io.Serializable;

/**
 * Getter that it used to get the current Timestamp.
 *
 * <p>This is mainly used for dependency injection during testing.
 */
public class CurrentTimestampGetter implements Serializable {

  CurrentTimestampGetter() {}

  public static CurrentTimestampGetter create() {
    return new CurrentTimestampGetter();
  }

  public Timestamp now() {
    return Timestamp.now();
  }

  public Timestamp nowPlusNanos(Integer nanos) {
    Timestamp currentTimestamp = Timestamp.now();
    return Timestamp.ofTimeSecondsAndNanos(
        currentTimestamp.getSeconds(), currentTimestamp.getNanos() + nanos);
  }
}
