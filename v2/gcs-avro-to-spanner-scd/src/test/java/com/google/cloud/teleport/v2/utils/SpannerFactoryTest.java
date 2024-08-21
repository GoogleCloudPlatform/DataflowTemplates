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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.utils.SpannerFactory.DatabaseClientManager;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerFactoryTest {

  private SpannerConfig spannerConfig;

  @Before
  public void setUp() {
    spannerConfig =
        SpannerConfig.create()
            .withHost(ValueProvider.StaticValueProvider.of("https://batch-spanner.googleapis.com"))
            .withProjectId("project-id")
            .withInstanceId("instance-id")
            .withDatabaseId("database-id");
  }

  @Test
  public void testGetDatabaseClient() {
    DatabaseClientManager databaseClientManager =
        SpannerFactory.withSpannerConfig(spannerConfig).getDatabaseClientManager();

    DatabaseClient databaseClient = databaseClientManager.getDatabaseClient();

    assertThat(databaseClient).isInstanceOf(DatabaseClient.class);
  }

  @Test
  public void testIsClosed_beforeClosing() {
    DatabaseClientManager databaseClientManager =
        SpannerFactory.withSpannerConfig(spannerConfig).getDatabaseClientManager();

    assertThat(databaseClientManager.isClosed()).isFalse();
  }

  @Test
  public void testIsClosed_afterClosing() {
    DatabaseClientManager databaseClientManager =
        SpannerFactory.withSpannerConfig(spannerConfig).getDatabaseClientManager();

    databaseClientManager.close();

    assertThat(databaseClientManager.isClosed()).isTrue();
  }
}
