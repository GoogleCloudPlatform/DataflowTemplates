/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.sourceddl;

/**
 * Interface for scanning information schema of source databases. Different implementations will
 * handle different source database types.
 */
public interface SourceInformationSchemaScanner {

  /**
   * Scans the source database's information schema and builds a {@link SourceSchema}.
   *
   * @return A {@link SourceSchema} representing the source database schema
   */
  SourceSchema scan();
}
