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
package com.google.cloud.teleport.spanner.spannerio.changestreams.cache;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.spanner.spannerio.changestreams.model.PartitionMetadata.State;
import javax.annotation.Nullable;

@FunctionalInterface
public interface WatermarkCache {

  /**
   * Fetches the earliest partition watermark from the partition metadata table that is not in a
   * {@link State#FINISHED} state.
   *
   * @return the earliest partition watermark which is not in a {@link State#FINISHED} state.
   */
  @Nullable
  Timestamp getUnfinishedMinWatermark();
}
