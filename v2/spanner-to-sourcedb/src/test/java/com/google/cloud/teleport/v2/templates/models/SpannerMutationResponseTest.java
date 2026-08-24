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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SpannerMutationResponseTest {

  @Test
  public void isEmptyReturnsTrueForNullMutation() {
    SpannerMutationResponse response = new SpannerMutationResponse(null, null);
    assertTrue(response.isEmpty());
    assertNull(response.getMutation());
    assertNull(response.getPrimaryKey());
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
    SpannerMutationResponse response = new SpannerMutationResponse(mutation, null);
    assertFalse(response.isEmpty());
    assertEquals(mutation, response.getMutation());
    assertNull(response.getPrimaryKey());
  }

  @Test
  public void twoArgumentConstructorStoresMutationAndPrimaryKey() {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(12)
            .set("Name")
            .to("John")
            .build();
    Key key = Key.of(12L);
    SpannerMutationResponse response = new SpannerMutationResponse(mutation, key);
    assertFalse(response.isEmpty());
    assertEquals(mutation, response.getMutation());
    assertEquals(key, response.getPrimaryKey());
  }

  @Test
  public void isEmptyReturnsTrueWhenMutationNullEvenIfKeyProvided() {
    Key key = Key.of(12L);
    SpannerMutationResponse response = new SpannerMutationResponse(null, key);
    assertTrue(response.isEmpty());
    assertNull(response.getMutation());
    assertEquals(key, response.getPrimaryKey());
  }
}
