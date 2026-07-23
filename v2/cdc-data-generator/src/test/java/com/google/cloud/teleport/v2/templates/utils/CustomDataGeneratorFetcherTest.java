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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CustomDataGeneratorFetcher}. */
@RunWith(JUnit4.class)
public class CustomDataGeneratorFetcherTest {

  @Test
  public void testNullOrEmptyArguments() {
    assertNull(CustomDataGeneratorFetcher.getCustomDataGenerator(null, null));
    assertNull(CustomDataGeneratorFetcher.getCustomDataGenerator("", "com.example.Class"));
    assertNull(CustomDataGeneratorFetcher.getCustomDataGenerator("gs://path", ""));
  }

  @Test
  public void testLoadFailsWithInvalidJar() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                CustomDataGeneratorFetcher.getCustomDataGenerator(
                    "gs://fake/jar.jar", "com.fake.Class"));
    assertTrue(exception.getMessage().contains("Failed to load CustomDataGenerator"));
  }

  @Test
  public void testLoadSuccess() {
    // URLClassLoader will delegate to the parent classloader, so it will find this
    // class
    // even if the dummy jar doesn't exist (JarFileReader will log a warning but
    // proceed).
    CustomDataGenerator generator =
        CustomDataGeneratorFetcher.getCustomDataGenerator(
            "dummy.jar", DummyGenerator.class.getName());
    assertTrue(generator instanceof DummyGenerator);
  }

  public static class DummyGenerator implements CustomDataGenerator {
    @Override
    public Object generate(String tableName, String columnName) {
      return null;
    }
  }
}
