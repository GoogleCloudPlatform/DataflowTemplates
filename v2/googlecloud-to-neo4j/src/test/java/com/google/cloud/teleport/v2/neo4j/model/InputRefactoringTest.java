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
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import org.junit.Test;

public class InputRefactoringTest {

  @Test
  public void interpolatesCustomTargetQueries() {
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"foo\": \"bar\"}");
    JobSpec jobSpec = new JobSpec();
    Target target = new Target();
    target.setType(TargetType.custom);
    target.setCustomQuery("RETURN \"$foo\"");
    jobSpec.getTargets().add(target);
    InputRefactoring refactorer = new InputRefactoring(options);

    refactorer.refactorJobSpec(jobSpec);

    assertThat(target.getCustomQuery()).isEqualTo("RETURN \"bar\"");
  }
}
