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
package com.google.cloud.teleport.it.conditions;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ChainedConditionCheck class provides a base interface for chaining {@link ConditionCheck}
 * checks. Each subsequent check will wait on the previous check to complete and return true.
 */
@AutoValue
public abstract class ChainedConditionCheck extends ConditionCheck {

  private static final Logger LOG = LoggerFactory.getLogger(ChainedConditionCheck.class);

  private int functionIndex = 0;

  abstract List<ConditionCheck> conditionChecks();

  @Override
  public String getDescription() {
    return String.format(
        "Chained condition check %d/%d...", functionIndex + 1, conditionChecks().size());
  }

  @Override
  public Boolean get() {
    while (conditionChecks().get(functionIndex).get()) {
      LOG.info(getDescription());
      functionIndex++;
    }
    return functionIndex == conditionChecks().size();
  }

  @Override
  protected CheckResult check() {
    throw new NotImplementedException("This check() should never be called.");
  }

  public static Builder builder(List<ConditionCheck> conditionChecks) {
    return new AutoValue_ChainedConditionCheck.Builder().setConditionChecks(conditionChecks);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setConditionChecks(List<ConditionCheck> setConditionChecks);

    abstract ChainedConditionCheck autoBuild();

    public ChainedConditionCheck build() {
      return autoBuild();
    }
  }
}
