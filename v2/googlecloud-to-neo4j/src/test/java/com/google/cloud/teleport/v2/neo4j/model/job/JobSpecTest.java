/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.job;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JobSpecTest {

  @Test
  public void sourceIterationOrderIsStable() {
    JobSpec jobSpec = new JobSpec();
    Map<String, Source> sources = jobSpec.getSources();
    Source sourceB = source("sourceB");
    sources.put(sourceB.getName(), sourceB);
    Source sourceA = source("sourceA");
    sources.put(sourceA.getName(), sourceA);

    assertThat(jobSpec.getSourceList()).isEqualTo(List.of(sourceB, sourceA));
  }

  private static Source source(String name) {
    Source source = new Source();
    source.setName(name);
    return source;
  }
}
