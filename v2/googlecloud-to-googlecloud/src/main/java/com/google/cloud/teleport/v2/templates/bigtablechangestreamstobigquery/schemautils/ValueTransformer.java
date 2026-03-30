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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import java.io.Serializable;

/** Transforms a Bigtable cell value from raw bytes to a string representation for BigQuery. */
public interface ValueTransformer extends Serializable {

  /**
   * Transforms raw cell value bytes to a string for BigQuery.
   *
   * @param bytes the raw cell value bytes
   * @return the transformed string, or null if transformation fails
   */
  String transform(byte[] bytes);
}
