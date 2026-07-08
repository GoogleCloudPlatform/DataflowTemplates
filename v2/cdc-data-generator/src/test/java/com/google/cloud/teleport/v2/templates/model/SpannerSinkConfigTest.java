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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerSinkConfigTest {

  @Test
  public void testSpannerSinkConfig_gettersSettersAndConstructors() {
    SpannerSinkConfig config1 = new SpannerSinkConfig();
    config1.setProjectId("projA");
    config1.setInstanceId("instA");
    config1.setDatabaseId("dbA");
    config1.setDialect(Dialect.GOOGLE_STANDARD_SQL);

    assertThat(config1.getProjectId()).isEqualTo("projA");
    assertThat(config1.getInstanceId()).isEqualTo("instA");
    assertThat(config1.getDatabaseId()).isEqualTo("dbA");
    assertThat(config1.getDialect()).isEqualTo(Dialect.GOOGLE_STANDARD_SQL);

    SpannerSinkConfig config2 =
        new SpannerSinkConfig("projA", "instA", "dbA", Dialect.GOOGLE_STANDARD_SQL);
    assertThat(config2).isEqualTo(config1);
    assertThat(config2.hashCode()).isEqualTo(config1.hashCode());
    assertThat(config2.toString()).contains("projA");

    SpannerSinkConfig config3 =
        new SpannerSinkConfig("projB", "instA", "dbA", Dialect.GOOGLE_STANDARD_SQL);
    assertThat(config3).isNotEqualTo(config1);
    assertThat(config1).isNotEqualTo(null);
    assertThat(config1).isNotEqualTo("some-string");
  }
}
