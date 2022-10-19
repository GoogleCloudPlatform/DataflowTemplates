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
package com.google.cloud.teleport.spanner;

/** GCS utilities. */
public class GcsUtil {
  private GcsUtil() {}

  public static String joinPath(String... paths) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      if (i > 0) {
        sb.append("/");
      }
      if (paths[i].endsWith("/")) {
        sb.append(paths[i].substring(0, paths[i].length() - 1));
      } else {
        sb.append(paths[i]);
      }
    }
    return sb.toString();
  }
}
