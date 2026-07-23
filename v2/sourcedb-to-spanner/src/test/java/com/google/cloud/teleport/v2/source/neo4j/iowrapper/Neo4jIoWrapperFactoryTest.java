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
package com.google.cloud.teleport.v2.source.neo4j.iowrapper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Unit tests for {@link Neo4jIoWrapperFactory}. */
public class Neo4jIoWrapperFactoryTest {

  @Test
  public void testFromPipelineOptions() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.NEO4J_SOURCE_DIALECT);
    options.setNeo4jUri("bolt://localhost:7687");
    options.setNeo4jUser("neo4j");
    options.setNeo4jPassword("password");
    options.setNeo4jReadChunkSize(100);

    Neo4jIoWrapperFactory factory = Neo4jIoWrapperFactory.fromPipelineOptions(options);
    assertNotNull(factory);
  }

  @Test
  public void testFromPipelineOptions_invalidDialect() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT);

    assertThrows(
        IllegalArgumentException.class, () -> Neo4jIoWrapperFactory.fromPipelineOptions(options));
  }

  @Test
  public void testFromPipelineOptions_missingUri() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.NEO4J_SOURCE_DIALECT);
    options.setNeo4jUser("neo4j");
    options.setNeo4jPassword("password");

    assertThrows(
        NullPointerException.class, () -> Neo4jIoWrapperFactory.fromPipelineOptions(options));
  }
}
