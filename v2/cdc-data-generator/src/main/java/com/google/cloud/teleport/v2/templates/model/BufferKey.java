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

/** Type-safe buffering key object for grouping row mutation lists. */
@AutoValue
public abstract class BufferKey implements Serializable {
  public abstract String tableName();

  public abstract String shardId();

  public abstract String operation();

  public static BufferKey create(String tableName, String shardId, String operation) {
    return new AutoValue_BufferKey(tableName, shardId, operation);
  }
}
