/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.utilities;

/**
 * Utilities to run test cases on derby
 *
 * @see org.apache.beam.it.jdbc.MySQLResourceManager
 */
public class DerbyUtils {

  public static String quote(String element) {
    return "\"" + element + "\"";
  }

  public static String modifyQuery(String query) {
    return query.replaceAll("`", "\"");
  }
}
