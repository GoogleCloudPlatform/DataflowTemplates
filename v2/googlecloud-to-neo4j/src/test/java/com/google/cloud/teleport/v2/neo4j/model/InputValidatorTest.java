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
package com.google.cloud.teleport.v2.neo4j.model;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.List;
import org.junit.Test;

public class InputValidatorTest {

  @Test
  public void rejectsCustomQueryTargetWithoutQuery() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must define a query");
  }

  @Test
  public void rejectsCustomQueryTargetWithMappings() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setCustomQuery("RETURN 42");
    target.setMappings(List.of(new Mapping()));
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must not define any mapping");
  }

  @Test
  public void rejectsCustomQueryTargetWithNonDefaultTransform() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setCustomQuery("RETURN 42");
    target.getTransform().setGroup(true);
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must not define any transform");
  }

  private static Source source(String name) {
    Source source = new Source();
    source.setName(name);
    return source;
  }
}
