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
package com.google.cloud.teleport.dfmetrics.output;

/** Class {@link OutputStoreFactory} creates appropriate output store based on the output type. */
public class OutputStoreFactory {

  private OutputStoreFactory() {}

  public static IOutputStore create(String outputType, String outputLocation) {
    switch (outputType.toUpperCase()) {
      case "BIGQUERY":
        return new BigqueryStore(outputLocation);
      case "FILE":
        return new FileStore(outputLocation);
      default:
        throw new IllegalArgumentException(
            "Invalid output type. Supported output types are: bigquery, file");
    }
  }
}
