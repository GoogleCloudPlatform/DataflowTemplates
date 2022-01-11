/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

import com.google.cloud.teleport.v2.utils.DualInputNestedValue.TranslatorInput;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for DualInputNestedValue. */
@RunWith(JUnit4.class)
public class DualInputNestedValueTest {
  @Rule public ExpectedException expectedException = org.junit.rules.ExpectedException.none();

  /** A test interface. */
  public interface TestOptions extends PipelineOptions {
    @Default.String("bar")
    String getBar();

    void setBar(String bar);

    Integer getFoo();

    void setFoo(Integer foo);
  }

  @Test
  public void testNestedValueStatic() throws Exception {
    String xvp = "foo";
    Integer yvp = 1;
    String zvp =
        DualInputNestedValue.of(
                xvp,
                yvp,
                new SerializableFunction<TranslatorInput<String, Integer>, String>() {
                  @Override
                  public String apply(TranslatorInput<String, Integer> input) {
                    return input.getX() + (input.getY() + 1);
                  }
                })
            .get();
    assertTrue(zvp != null);
    assertEquals("foo2", zvp);
  }
}
