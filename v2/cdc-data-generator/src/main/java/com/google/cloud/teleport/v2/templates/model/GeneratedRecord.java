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
package com.google.cloud.teleport.v2.templates.model;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.values.Row;

/** Type-safe container wrapping table names and primary key values. */
@AutoValue
public abstract class GeneratedRecord implements Serializable {
  public abstract String tableName();

  public abstract Row primaryKeyValues();

  public static GeneratedRecord create(String tableName, Row primaryKeyValues) {
    return new AutoValue_GeneratedRecord(tableName, primaryKeyValues);
  }
}
