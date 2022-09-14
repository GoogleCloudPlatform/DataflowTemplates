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
package com.google.cloud.teleport.spanner;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Tests for GcsUtil class. */
public class GcsUtilTest {

  @Test
  public void testJoinPath() {
    assertEquals(
        "gs://testBucket/folder1/folder2/folder3",
        GcsUtil.joinPath("gs://testBucket", "folder1", "folder2/", "folder3"));
  }
}
