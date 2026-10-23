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
package com.google.cloud.teleport.v2.source;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.mysql.MySqlSourceConnector;
import com.google.cloud.teleport.v2.source.spanner.SpannerSourceConnector;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SourceConnectorFactory}. */
@RunWith(JUnit4.class)
public class SourceConnectorFactoryTest {

  @Test
  public void testGetSourceConnector_bySinkType() {
    assertThat(SourceConnectorFactory.getSourceConnector(SinkType.SPANNER))
        .isInstanceOf(SpannerSourceConnector.class);
    assertThat(SourceConnectorFactory.getSourceConnector(SinkType.MYSQL))
        .isInstanceOf(MySqlSourceConnector.class);
  }

  @Test
  public void testGetSourceConnector_bySinkType_nullThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnector((SinkType) null));
  }

  @Test
  public void testGetSourceConnector_byOptions() {
    CdcDataGeneratorOptions options = PipelineOptionsFactory.as(CdcDataGeneratorOptions.class);
    options.setSinkType(SinkType.SPANNER);
    assertThat(SourceConnectorFactory.getSourceConnector(options))
        .isInstanceOf(SpannerSourceConnector.class);

    options.setSinkType(SinkType.MYSQL);
    assertThat(SourceConnectorFactory.getSourceConnector(options))
        .isInstanceOf(MySqlSourceConnector.class);
  }

  @Test
  public void testGetSourceConnector_byOptions_nullOptionsOrSinkTypeThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnector((CdcDataGeneratorOptions) null));

    CdcDataGeneratorOptions options = PipelineOptionsFactory.as(CdcDataGeneratorOptions.class);
    assertThrows(
        IllegalArgumentException.class, () -> SourceConnectorFactory.getSourceConnector(options));
  }

  @Test
  public void testGetSourceConnectorBySourceType() {
    assertThat(SourceConnectorFactory.getSourceConnectorBySourceType("SPANNER"))
        .isInstanceOf(SpannerSourceConnector.class);
    assertThat(SourceConnectorFactory.getSourceConnectorBySourceType("MYSQL"))
        .isInstanceOf(MySqlSourceConnector.class);
    assertThat(SourceConnectorFactory.getSourceConnectorBySourceType("spanner"))
        .isInstanceOf(SpannerSourceConnector.class);
    assertThat(SourceConnectorFactory.getSourceConnectorBySourceType("mysql"))
        .isInstanceOf(MySqlSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorBySourceType_invalidThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnectorBySourceType(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnectorBySourceType(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnectorBySourceType("   "));
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnectorBySourceType("ORACLE"));
  }
}
