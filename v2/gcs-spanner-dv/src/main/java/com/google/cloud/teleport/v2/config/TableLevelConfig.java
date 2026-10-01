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
package com.google.cloud.teleport.v2.config;

import java.io.Serializable;

/**
 * Placeholder POJO representing future advanced configurations for a specific table.
 *
 * <p>This is intended to support features like column-level validation or deterministic sampling in
 * the future.
 */
public class TableLevelConfig implements Serializable {

  // Intentionally left empty for now.
  //
  // Example future fields:
  // private List<String> columnsToValidate;
  // private SamplingConfig sampling;

}
