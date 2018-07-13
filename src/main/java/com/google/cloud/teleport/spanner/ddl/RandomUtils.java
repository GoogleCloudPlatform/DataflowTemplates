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

package com.google.cloud.teleport.spanner.ddl;

import java.util.Random;

/** Utilities for {@link Random}. */
public final class RandomUtils {

  private static final String ALPHA_LOWER = "abcdefghijklmnopqrstuvwxyz";
  private static final String ALPHA = ALPHA_LOWER + ALPHA_LOWER.toUpperCase();
  private static final String NUMERIC = "1234567890";
  private static final String ALPHANUMERIC = ALPHA + NUMERIC;
  private static final String UTF8 = ALPHANUMERIC + "абвгдежзйцифхяж";

  private RandomUtils() {}

  public static String randomAlphanumeric(int length) {
    Random random = new Random();
    char[] result = new char[length];
    result[0] = ALPHA.charAt(random.nextInt(ALPHA.length()));
    for (int i = 1; i < length; i++) {
      result[i] = ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length()));
    }
    return new String(result);
  }

  public static String randomUtf8(int length) {
    Random random = new Random();
    String result = "";
    result += ALPHA.charAt(random.nextInt(ALPHA.length()));
    for (int i = 1; i < length; i++) {
      result += UTF8.charAt(random.nextInt(UTF8.length()));
    }
    return result;
  }
}
