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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.importer.v1.targets.Target;

public class TargetSequence implements Serializable {

  private final Map<String, Integer> targetSequences = new HashMap<>();
  private final AtomicInteger nextNumber = new AtomicInteger(0);

  public int getSequenceNumber(Target target) {
    return targetSequences.computeIfAbsent(target.getName(), (key) -> nextNumber.getAndIncrement());
  }
}
