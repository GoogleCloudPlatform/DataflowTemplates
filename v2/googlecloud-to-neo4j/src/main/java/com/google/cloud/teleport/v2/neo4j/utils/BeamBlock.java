/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility function to queue PCollections, flatten and return empty queue on demand. */
public class BeamBlock {

  private static final Logger LOG = LoggerFactory.getLogger(BeamBlock.class);
  private final List<PCollection<Row>> sourceQueue = new ArrayList<>();
  private final List<PCollection<Row>> preloadActionQueue = new ArrayList<>();
  private final List<PCollection<Row>> processActionQueue = new ArrayList<>();
  private final List<PCollection<Row>> nodeQueue = new ArrayList<>();
  private final List<PCollection<Row>> edgeQueue = new ArrayList<>();
  private final Map<String, PCollection<Row>> executeAfterNamedQueue = new HashMap<>();
  private final Map<String, PCollection<Row>> executionContexts = new HashMap<>();
  private PCollection<Row> defaultCollection;

  private BeamBlock() {}

  public BeamBlock(PCollection<Row> defaultCollection) {
    this.defaultCollection = defaultCollection;
  }

  public void addToQueue(
      ArtifactType artifactType,
      boolean preload,
      String name,
      PCollection<Row> blockingReturn,
      PCollection<Row> executionContext) {
    if (artifactType == ArtifactType.action) {
      if (preload) {
        preloadActionQueue.add(blockingReturn);
      } else {
        processActionQueue.add(blockingReturn);
      }
    } else if (artifactType == ArtifactType.source) {
      sourceQueue.add(blockingReturn);
    } else if (artifactType == ArtifactType.node) {
      nodeQueue.add(blockingReturn);
    } else if (artifactType == ArtifactType.edge) {
      edgeQueue.add(blockingReturn);
    }
    executeAfterNamedQueue.put(artifactType.name() + ":" + name, blockingReturn);
    executionContexts.put(artifactType.name() + ":" + name, executionContext);
  }

  public PCollection<Row> getContextCollection(ArtifactType artifactType, String name) {
    if (executionContexts.containsKey(artifactType + ":" + name)) {
      // execution context has been registered
      return executionContexts.get(artifactType + ":" + name);
    }
    return defaultCollection;
  }

  public PCollection<Row> waitOnCollection(
      ActionExecuteAfter executeAfter, String executeAfterName, String queuingDescription) {
    List<PCollection<Row>> waitOnQueues = new ArrayList<>();
    if (executeAfter == ActionExecuteAfter.start) {
      // no dependencies
    } else if (executeAfter == ActionExecuteAfter.preloads) {
      waitOnQueues.addAll(preloadActionQueue);
    } else if (executeAfter == ActionExecuteAfter.sources) {
      waitOnQueues.addAll(sourceQueue);
    } else if (executeAfter == ActionExecuteAfter.nodes) {
      waitOnQueues.addAll(nodeQueue);
      if (waitOnQueues.isEmpty()) {
        waitOnQueues.addAll(sourceQueue);
      }
      // end is same as after edges
    } else if (executeAfter == ActionExecuteAfter.edges || executeAfter == ActionExecuteAfter.loads) {
      waitOnQueues.addAll(edgeQueue);
      if (waitOnQueues.isEmpty()) {
        waitOnQueues.addAll(nodeQueue);
      }
      if (waitOnQueues.isEmpty()) {
        waitOnQueues.addAll(sourceQueue);
      }
    } else if (!StringUtils.isEmpty(executeAfterName)) {
      if (executeAfter==ActionExecuteAfter.node) {
        waitOnQueues.add(executeAfterNamedQueue.get(ArtifactType.node.name() + ":" + executeAfterName));
      } else if (executeAfter==ActionExecuteAfter.edge) {
        waitOnQueues.add(executeAfterNamedQueue.get(ArtifactType.edge.name() + ":" + executeAfterName));
      } else if (executeAfter==ActionExecuteAfter.action) {
        waitOnQueues.add(executeAfterNamedQueue.get(ArtifactType.action.name() + ":" + executeAfterName));
      } else if (executeAfter==ActionExecuteAfter.source) {
        waitOnQueues.add(executeAfterNamedQueue.get(ArtifactType.source.name() + ":" + executeAfterName));
      }
    }
    if (waitOnQueues.isEmpty()) {
      waitOnQueues.add(defaultCollection);
    }

    LOG.info("Queue: "+queuingDescription+", executeAfter: "+executeAfter.name()+", executeAfterName: "+executeAfterName+", waiting on "+waitOnQueues.size()+" queues");
    return PCollectionList.of(waitOnQueues)
        .apply("** Waiting "+queuingDescription+" (after " + executeAfter.name()+"/"+executeAfterName+")", Flatten.pCollections());
  }
}
