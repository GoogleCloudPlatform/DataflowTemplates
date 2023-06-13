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
package com.google.cloud.teleport.it.datadog.conditions;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.common.conditions.ConditionCheck;
import com.google.cloud.teleport.it.datadog.DatadogResourceManager;
import javax.annotation.Nullable;

/** ConditionCheck to validate if Datadog has received a certain amount of events. */
@AutoValue
public abstract class DatadogLogEntriesCheck extends ConditionCheck {

  abstract DatadogResourceManager resourceManager();

  abstract Integer minEntries();

  @Nullable
  abstract Integer maxEntries();

  @Override
  public String getDescription() {
    if (maxEntries() != null) {
      return String.format(
          "Datadog check if logs have between %d and %d events", minEntries(), maxEntries());
    }
    return String.format("Datadog check if logs have %d events", minEntries());
  }

  @Override
  public CheckResult check() {
    long totalEvents = resourceManager().getEntries().size();

    if (totalEvents < minEntries()) {
      return new CheckResult(
          false, String.format("Expected %d but has only %d", minEntries(), totalEvents));
    }
    if (maxEntries() != null && totalEvents > maxEntries()) {
      return new CheckResult(
          false, String.format("Expected up to %d but found %d events", maxEntries(), totalEvents));
    }

    if (maxEntries() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d events and found %d",
              minEntries(), maxEntries(), totalEvents));
    }

    return new CheckResult(
        true, String.format("Expected at least %d events and found %d", minEntries(), totalEvents));
  }

  public static Builder builder(DatadogResourceManager resourceManager) {
    return new AutoValue_DatadogLogEntriesCheck.Builder().setResourceManager(resourceManager);
  }

  /** Builder for {@link DatadogLogEntriesCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(DatadogResourceManager resourceManager);

    public abstract Builder setMinEntries(Integer minEvents);

    public abstract Builder setMaxEntries(Integer maxEvents);

    abstract DatadogLogEntriesCheck autoBuild();

    public DatadogLogEntriesCheck build() {
      return autoBuild();
    }
  }
}
