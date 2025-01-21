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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

public class CassandraDefaults {

  /** Fluent Backoff for Cassandra Schema Discovery. */
  public static final FluentBackoff DEFAULT_CASSANDRA_SCHEMA_DISCOVERY_BACKOFF =
      FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));

  private CassandraDefaults() {}
  ;
}
