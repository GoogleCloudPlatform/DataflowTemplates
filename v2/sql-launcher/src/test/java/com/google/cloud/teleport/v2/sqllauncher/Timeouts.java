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
package com.google.cloud.teleport.v2.sqllauncher;

import org.joda.time.Duration;

/** Common timeouts for other tests. */
final class Timeouts {

  // Allowed delay from when a run is requested until workers have started.
  static final Duration STARTUP = Duration.standardMinutes(60);

  // Allowed delay from when a pipline is finished until it has completely shutdown.
  static final Duration SHUTDOWN = Duration.standardMinutes(60);

  // Total allowed overhead (startup delay + shutdown delay).
  static final Duration OVERHEAD = STARTUP.plus(SHUTDOWN);

  private Timeouts() {}
}
