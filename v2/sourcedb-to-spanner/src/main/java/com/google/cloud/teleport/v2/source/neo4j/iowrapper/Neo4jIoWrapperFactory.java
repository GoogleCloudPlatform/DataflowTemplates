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

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait.OnSignal;

/** Factory to construct {@link Neo4jIoWrapper}. */
public class Neo4jIoWrapperFactory implements IoWrapperFactory, Serializable {

  private final String neo4jUri;
  private final String neo4jUser;
  private final String neo4jPassword;
  private final int readChunkSize;

  public Neo4jIoWrapperFactory(
      String neo4jUri, String neo4jUser, String neo4jPassword, int readChunkSize) {
    this.neo4jUri = neo4jUri;
    this.neo4jUser = neo4jUser;
    this.neo4jPassword = neo4jPassword;
    this.readChunkSize = readChunkSize;
  }

  public static Neo4jIoWrapperFactory fromPipelineOptions(SourceDbToSpannerOptions options) {
    Preconditions.checkArgument(
        options.getSourceDbDialect().equals(SourceDbToSpannerOptions.NEO4J_SOURCE_DIALECT),
        "Unexpected Dialect " + options.getSourceDbDialect() + " for Neo4j Source");
    Preconditions.checkNotNull(options.getNeo4jUri(), "Neo4j URI must be specified");
    Preconditions.checkNotNull(options.getNeo4jUser(), "Neo4j Username must be specified");
    Preconditions.checkNotNull(options.getNeo4jPassword(), "Neo4j Password must be specified");

    return new Neo4jIoWrapperFactory(
        options.getNeo4jUri(),
        options.getNeo4jUser(),
        options.getNeo4jPassword(),
        options.getNeo4jReadChunkSize());
  }

  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, OnSignal<?> waitOnSignal) {
    return new Neo4jIoWrapper(neo4jUri, neo4jUser, neo4jPassword, readChunkSize, sourceTables);
  }
}
