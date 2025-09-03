/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql.conditions;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;

/** ConditionCheck to validate if CloudSQL has received a certain amount of rows. */
@AutoValue
public abstract class CloudSQLRowsCheck extends ConditionCheck {

  abstract CloudSqlResourceManager resourceManager();

  abstract String tableId();

  abstract Integer minRows();

  @Nullable
  abstract Integer maxRows();

  @Override
  public String getDescription() {
    if (maxRows() != null) {
      return String.format(
          "CloudSQL check if table %s has between %d and %d rows", tableId(), minRows(), maxRows());
    }
    return String.format("CloudSQL check if table %s has %d rows", tableId(), minRows());
  }

  @Override
  public CheckResult check() {
    long totalRows = resourceManager().getRowCount(tableId());
    if (totalRows < minRows()) {
      return new CheckResult(
          false, String.format("Expected %d rows but has only %d", minRows(), totalRows));
    }
    if (maxRows() != null && totalRows > maxRows()) {
      return new CheckResult(
          false, String.format("Expected up to %d rows but found %d", maxRows(), totalRows));
    }

    if (maxRows() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d rows and found %d", minRows(), maxRows(), totalRows));
    }

    return new CheckResult(
        true, String.format("Expected at least %d rows and found %d", minRows(), totalRows));
  }

  public static Builder builder(CloudSqlResourceManager resourceManager, String tableId) {
    return new AutoValue_CloudSQLRowsCheck.Builder()
        .setResourceManager(resourceManager)
        .setTableId(tableId);
  }

  /** Builder for {@link CloudSQLRowsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(CloudSqlResourceManager resourceManager);

    public abstract Builder setTableId(String tableId);

    public abstract Builder setMinRows(Integer minRows);

    public abstract Builder setMaxRows(Integer maxRows);

    abstract CloudSQLRowsCheck autoBuild();

    public CloudSQLRowsCheck build() {
      return autoBuild();
    }
  }
}
