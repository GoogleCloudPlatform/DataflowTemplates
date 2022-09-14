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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.junit.Test;

/** Tests for AsList class. */
public class AsListTest {

  @Test
  public void testAsList() {
    CombineFn<String, List<String>, List<String>> asList = AsList.fn();

    List<String> accumulator1 = asList.createAccumulator();
    assertArrayEquals(new String[] {"input1"}, asList.addInput(accumulator1, "input1").toArray());
    List<String> accumulator2 = asList.createAccumulator();
    assertArrayEquals(new String[] {"input2"}, asList.addInput(accumulator2, "input2").toArray());

    assertArrayEquals(
        new String[] {"input1", "input2"},
        asList.mergeAccumulators(Arrays.asList(accumulator1, accumulator2)).toArray());

    assertEquals(accumulator1, asList.extractOutput(accumulator1));
    assertNotNull(new AsList());
  }
}
