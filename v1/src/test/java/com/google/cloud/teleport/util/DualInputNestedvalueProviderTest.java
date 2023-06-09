/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.util;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for DualInputNestedValueProvider. */
@RunWith(JUnit4.class)
public class DualInputNestedvalueProviderTest {
  @Rule public ExpectedException expectedException = org.junit.rules.ExpectedException.none();

  /** A test interface. */
  public interface TestOptions extends PipelineOptions {
    @Default.String("bar")
    ValueProvider<String> getBar();

    void setBar(ValueProvider<String> bar);

    ValueProvider<Integer> getFoo();

    void setFoo(ValueProvider<Integer> foo);
  }

  @Test
  public void testNestedValueProviderStatic() throws Exception {
    ValueProvider<String> xvp = StaticValueProvider.of("foo");
    ValueProvider<Integer> yvp = StaticValueProvider.of(1);
    ValueProvider<String> zvp =
        DualInputNestedValueProvider.of(
            xvp,
            yvp,
            new SerializableFunction<TranslatorInput<String, Integer>, String>() {
              @Override
              public String apply(TranslatorInput<String, Integer> input) {
                return input.getX() + (input.getY() + 1);
              }
            });
    assertTrue(zvp.isAccessible());
    assertEquals("foo2", zvp.get());
  }

  @Test
  public void testNestedValueProviderRuntime() throws Exception {
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    ValueProvider<String> bar = options.getBar();
    ValueProvider<Integer> foo = options.getFoo();
    ValueProvider<String> barfoo =
        DualInputNestedValueProvider.of(
            bar,
            foo,
            new SerializableFunction<TranslatorInput<String, Integer>, String>() {
              @Override
              public String apply(TranslatorInput<String, Integer> input) {
                return input.getX() + (input.getY() + 1);
              }
            });
    assertFalse(barfoo.isAccessible());
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Value only available at runtime");
    barfoo.get();
  }
}
