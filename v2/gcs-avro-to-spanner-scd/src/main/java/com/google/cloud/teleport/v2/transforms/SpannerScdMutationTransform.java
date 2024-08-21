/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import com.google.cloud.teleport.v2.utils.CurrentTimestampGetter;
import com.google.cloud.teleport.v2.utils.SpannerFactory;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Writes batch rows into Spanner using the defined SCD Type.
 *
 * <ul>
 *   <li>SCD Type 1: if primary key(s) exist, updates the existing row; it inserts a new row
 *       otherwise.
 *   <li>SCD Type 2: if primary key(s) exist, updates the end timestamp to the current timestamp.
 *       Note: since end timestamp is part of the primary key, it requires delete and insert to
 *       achieve this. In all cases, it inserts a new row with the new data and null end timestamp.
 *       If start timestamp column is specified, it sets it to the current timestamp when inserting.
 * </ul>
 */
@AutoValue
public abstract class SpannerScdMutationTransform
    extends PTransform<PCollection<Iterable<Struct>>, PDone> {

  abstract ScdType scdType();

  abstract SpannerConfig spannerConfig();

  abstract String tableName();

  @Nullable
  abstract List<String> primaryKeyColumnNames();

  @Nullable
  abstract String startDateColumnName();

  @Nullable
  abstract String endDateColumnName();

  abstract ImmutableList<String> tableColumnNames();

  abstract SpannerFactory spannerFactory();

  abstract CurrentTimestampGetter currentTimestampGetter();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setScdType(ScdType value);

    public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

    public abstract Builder setTableName(String value);

    public abstract Builder setPrimaryKeyColumnNames(List<String> value);

    public abstract Builder setStartDateColumnName(String value);

    public abstract Builder setEndDateColumnName(String value);

    public abstract Builder setTableColumnNames(ImmutableList<String> value);

    public Builder setTableColumnNames(Iterable<String> columnNames) {
      return setTableColumnNames(ImmutableList.copyOf(columnNames));
    }

    public abstract Builder setSpannerFactory(SpannerFactory spannerFactory);

    public abstract Builder setCurrentTimestampGetter(
        CurrentTimestampGetter currentTimestampGetter);

    public abstract SpannerScdMutationTransform build();
  }

  public static Builder builder() {
    return new AutoValue_SpannerScdMutationTransform.Builder();
  }

  /**
   * Writes data to Spanner using the required SCD Type.
   *
   * @param input
   * @return Pipeline is completed.
   */
  @Override
  public PDone expand(PCollection<Iterable<Struct>> input) {
    String scdTypeName = scdType().toString().toLowerCase().replace("_", "");
    String stepName =
        String.format(
            "WriteScd%sToSpanner",
            scdTypeName.substring(0, 1).toUpperCase() + scdTypeName.substring(1));

    input.apply(
        stepName,
        ParDo.of(
            SpannerScdMutationDoFn.builder()
                .setScdType(scdType())
                .setSpannerConfig(spannerConfig())
                .setTableName(tableName())
                .setPrimaryKeyColumnNames(primaryKeyColumnNames())
                .setStartDateColumnName(startDateColumnName())
                .setEndDateColumnName(endDateColumnName())
                .setTableColumnNames(tableColumnNames())
                .setSpannerFactory(spannerFactory())
                .setCurrentTimestampGetter(currentTimestampGetter())
                .build()));
    return PDone.in(input.getPipeline());
  }
}
