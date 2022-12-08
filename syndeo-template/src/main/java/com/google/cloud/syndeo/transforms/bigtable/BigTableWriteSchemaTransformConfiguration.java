/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo.transforms.bigtable;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigTableWriteSchemaTransformConfiguration {
  public abstract String getProjectId();

  public abstract String getInstanceId();

  public abstract String getTableId();

  public abstract List<String> getKeyColumns();

  public abstract @Nullable String getEndpoint();

  public abstract @Nullable String getAppProfileId();

  public static Builder builder() {
    return new AutoValue_BigTableWriteSchemaTransformConfiguration.Builder();
  }

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigTableWriteSchemaTransformConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigTableWriteSchemaTransformConfiguration.class);
  private static final SerializableFunction<BigTableWriteSchemaTransformConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  /** Transform configuration to a {@link Row}. */
  public Row toBeamRow() {
    return ROW_SERIALIZABLE_FUNCTION.apply(this);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String value);

    public abstract Builder setInstanceId(String value);

    public abstract Builder setTableId(String value);

    public abstract Builder setKeyColumns(List<String> value);

    public abstract Builder setEndpoint(String endpoint);

    public abstract Builder setAppProfileId(String appProfile);

    public abstract BigTableWriteSchemaTransformConfiguration build();
  }
}
