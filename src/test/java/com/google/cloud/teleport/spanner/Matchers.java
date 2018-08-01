/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.spanner;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** A set of matchers. */
public class Matchers {

  private Matchers() {}

  public static TypeSafeMatcher<String> equalsIgnoreWhitespace(final String str) {
    final String expected = stripWhitespace(str);
    return new TypeSafeMatcher<String>() {

      @Override
      protected boolean matchesSafely(String item) {
        item = stripWhitespace(item);
        return expected.equals(item);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("String equals without whitespaces to " + str);
      }
    };
  }

  private static String stripWhitespace(String str) {
    return str.replaceAll("\\s+", "");
  }
}
