/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import com.google.cloud.spanner.Dialect;

/**
 * Configuration for a Cloud Spanner sink, representing both the parsed JSON config file and the
 * pre-fetched dialect.
 *
 * @deprecated Use {@link com.google.cloud.teleport.v2.source.spanner.SpannerSinkConfig} instead.
 */
@Deprecated
public class SpannerSinkConfig
    extends com.google.cloud.teleport.v2.source.spanner.SpannerSinkConfig {

  public SpannerSinkConfig() {
    super();
  }

  public SpannerSinkConfig(
      String projectId, String instanceId, String databaseId, Dialect dialect) {
    super(projectId, instanceId, databaseId, dialect);
  }
}
