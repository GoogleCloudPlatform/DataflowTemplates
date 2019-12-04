/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.applier;

import com.google.pubsub.v1.ProjectTopicName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class PubSubUtilsTest {

  @Test
  public void testTopicComesCorrectly() {
    ProjectTopicName ptn = ProjectTopicName.parse("projects/myproject/topics/mytopic");

    assertThat(ptn.getProject(), is("myproject"));
    assertThat(ptn.getTopic(), is ("mytopic"));
  }



}
