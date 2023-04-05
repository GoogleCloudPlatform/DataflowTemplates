/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.common;

/** Utility class to search for certain error message in the exception chain. */
public class ExceptionMessageUtils {

  private ExceptionMessageUtils() {
    // don't create
  }

  public static boolean underlyingErrorContains(Throwable t, String messageToLookFor) {
    while (t != null) {
      String msg = t.getMessage();
      if (msg != null && msg.contains(messageToLookFor)) {
        return true;
      } else {
        t = t.getCause();
      }
    }
    return false;
  }
}
