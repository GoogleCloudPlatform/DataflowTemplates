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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import java.util.function.Predicate;

public abstract class CommitTimeAwarePredicate implements Predicate<byte[]> {

  private Long min = null;
  private Long max = null;

  public void observeCommitTime(long commitTimeMicros) {
    if (min == null || max == null) {
      max = min = commitTimeMicros;
    } else {
      if (min > commitTimeMicros) {
        min = commitTimeMicros;
      }
      if (max < commitTimeMicros) {
        max = commitTimeMicros;
      }
    }
  }

  public Long getEarliestCommitTime() {
    return min;
  }

  public Long getLatestCommitTime() {
    return max;
  }

  public boolean found() {
    return min != null;
  }
}
