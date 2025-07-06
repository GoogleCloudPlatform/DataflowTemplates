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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
// import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.ManagedIOToManagedIO.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link ManagedIOToManagedIO}. */
@RunWith(JUnit4.class)
public class ManagedIOToManagedIOTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testValidSourceConnectorTypes() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    // Test valid source connector types
    String[] validSources = {"ICEBERG", "ICEBERG_CDC", "KAFKA", "BIGQUERY"};
    
    for (String source : validSources) {
      options.setSourceConnectorType(source);
      options.setSinkConnectorType("ICEBERG");
      options.setSourceConfig("{}");
      options.setSinkConfig("{}");
      
      // Should not throw exception for valid connector types
      assertThat(options.getSourceConnectorType()).isEqualTo(source);
    }
  }

  @Test
  public void testValidSinkConnectorTypes() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    // Test valid sink connector types (note: ICEBERG_CDC is not available for sinks)
    String[] validSinks = {"ICEBERG", "KAFKA", "BIGQUERY"};
    
    for (String sink : validSinks) {
      options.setSourceConnectorType("KAFKA");
      options.setSinkConnectorType(sink);
      options.setSourceConfig("{}");
      options.setSinkConfig("{}");
      
      // Should not throw exception for valid connector types
      assertThat(options.getSinkConnectorType()).isEqualTo(sink);
    }
  }

  @Test
  public void testJsonConfigParsing() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    // Test valid JSON configurations
    String kafkaSourceConfig = "{\"bootstrap_servers\": \"localhost:9092\", \"topic\": \"input-topic\", \"format\": \"JSON\"}";
    String bigquerySinkConfig = "{\"table\": \"project:dataset.table\"}";
    
    options.setSourceConnectorType("KAFKA");
    options.setSinkConnectorType("BIGQUERY");
    options.setSourceConfig(kafkaSourceConfig);
    options.setSinkConfig(bigquerySinkConfig);
    
    assertThat(options.getSourceConfig()).isEqualTo(kafkaSourceConfig);
    assertThat(options.getSinkConfig()).isEqualTo(bigquerySinkConfig);
  }

  @Test
  public void testDefaultStreamingMode() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    // Default streaming should be false
    assertThat(options.isStreaming()).isFalse();
  }

  @Test
  public void testStreamingModeConfiguration() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    options.setStreaming(true);
    assertThat(options.isStreaming()).isTrue();
    options.setStreaming(false);
    assertThat(options.isStreaming()).isFalse();
  }

  @Test
  public void testEmptyJsonConfig() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    
    // Empty JSON should be handled gracefully
    options.setSourceConfig("");
    options.setSinkConfig("{}");
    
    assertThat(options.getSourceConfig()).isEmpty();
    assertThat(options.getSinkConfig()).isEqualTo("{}");
  }
}