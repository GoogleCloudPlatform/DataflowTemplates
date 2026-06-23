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
package com.google.cloud.teleport.v2.templates.models;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SpannerMutationResponseTest {

  @Test
  public void isEmptyReturnsTrueForNullMutation() {
    SpannerMutationResponse response = new SpannerMutationResponse(null);
    assertTrue(response.isEmpty());
  }

  @Test
  public void isEmptyReturnsFalseForValidMutation() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(12)
            .set("Name")
            .to("John")
            .build();
    SpannerMutationResponse response = new SpannerMutationResponse(mutation);
    assertFalse(response.isEmpty());
  }
}
