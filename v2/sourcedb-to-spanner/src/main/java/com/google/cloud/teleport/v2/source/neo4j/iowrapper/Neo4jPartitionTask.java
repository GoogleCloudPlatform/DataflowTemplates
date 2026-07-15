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

import java.io.Serializable;

/** Describes a bounded node/edge ID query partition task. */
public class Neo4jPartitionTask implements Serializable {
  private final String elementType; // "node" or "edge"
  private final long startId;
  private final long endId;

  public Neo4jPartitionTask(String elementType, long startId, long endId) {
    this.elementType = elementType;
    this.startId = startId;
    this.endId = endId;
  }

  public String getElementType() {
    return elementType;
  }

  public long getStartId() {
    return startId;
  }

  public long getEndId() {
    return endId;
  }
}
