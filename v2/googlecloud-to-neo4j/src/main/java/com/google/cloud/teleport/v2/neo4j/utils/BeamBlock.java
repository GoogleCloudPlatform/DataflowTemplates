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

import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility function to queue PCollections, flatten and return empty queue on demand. */
public class BeamBlock {

  private static final Logger LOG = LoggerFactory.getLogger(BeamBlock.class);
  private final Map<String, PCollection<Row>> outputs = new HashMap<>();
  private final PCollection<Row> defaultCollection;

  public BeamBlock(PCollection<Row> defaultCollection) {
    this.defaultCollection = defaultCollection;
  }

  public void addToQueue(ArtifactType artifactType, String name, PCollection<Row> output) {
    outputs.put(artifactType.name() + ":" + name, output);
  }

  public PCollection<Row> resolveOutputs(
      Collection<String> dependencies, String queuingDescription) {
    List<PCollection<Row>> waitOnQueues = resolveOutputs(dependencies);
    if (waitOnQueues.isEmpty()) {
      waitOnQueues.add(defaultCollection);
    }

    LOG.info(
        "Queue: "
            + queuingDescription
            + ", dependencies: "
            + String.join(", ", dependencies)
            + ", waiting on "
            + waitOnQueues.size()
            + " queues");
    return PCollectionList.of(waitOnQueues)
        .apply(
            "** Waiting " + queuingDescription + " (after " + String.join(", ", dependencies) + ")",
            Flatten.pCollections());
  }

  private List<PCollection<Row>> resolveOutputs(Collection<String> dependencies) {
    List<PCollection<Row>> outputs = new ArrayList<>();
    for (String dependency : dependencies) {
      for (ArtifactType type : ArtifactType.values()) {
        if (this.outputs.containsKey(type + ":" + dependency)) {
          outputs.add(this.outputs.get(type + ":" + dependency));
          break;
        }
      }
    }
    return outputs;
  }
}
