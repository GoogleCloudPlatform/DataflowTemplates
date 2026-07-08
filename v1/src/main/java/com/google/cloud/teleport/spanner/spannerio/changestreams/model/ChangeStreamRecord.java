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
package com.google.cloud.teleport.spanner.spannerio.changestreams.model;

import com.google.cloud.Timestamp;
import java.io.Serializable;

/**
 * Represents a Spanner Change Stream Record. It can be one of: {@link DataChangeRecord}, {@link
 * HeartbeatRecord} or {@link ChildPartitionsRecord}.
 */
public interface ChangeStreamRecord extends Serializable {

  /**
   * The timestamp associated with the record. It will come from a different value depending on the
   * underlying record implementation.
   *
   * @return the timestamp at which this record has occurred.
   */
  Timestamp getRecordTimestamp();
}
