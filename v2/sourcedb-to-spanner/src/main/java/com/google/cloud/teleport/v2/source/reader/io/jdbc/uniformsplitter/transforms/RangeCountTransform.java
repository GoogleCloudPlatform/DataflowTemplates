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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

/** PTransform to wrap {@link RangeCountDoFn}. */
@AutoValue
public abstract class RangeCountTransform extends PTransform<PCollection<Range>, PCollection<Range>>
    implements Serializable {

  /** Provider for {@link DataSource}. */
  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  /**
   * Implementations of {@link UniformSplitterDBAdapter} to get queries as per the dialect of the
   * database.
   */
  abstract UniformSplitterDBAdapter dbAdapter();

  /** Timeout of the count query in milliseconds. */
  abstract long timeoutMillis();

  /** Name of the table. */
  abstract String tableName();

  /** List of partition columns. */
  abstract ImmutableList<String> partitionColumns();

  @Override
  public PCollection<Range> expand(PCollection<Range> input) {
    return input.apply(
        ParDo.of(
            new RangeCountDoFn(
                dataSourceProviderFn(),
                timeoutMillis(),
                dbAdapter().getCountQuery(tableName(), partitionColumns(), timeoutMillis()),
                partitionColumns().size())));
  }

  public static Builder builder() {
    return new AutoValue_RangeCountTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataSourceProviderFn(SerializableFunction<Void, DataSource> value);

    public abstract Builder setDbAdapter(UniformSplitterDBAdapter value);

    public abstract Builder setTimeoutMillis(long value);

    public abstract Builder setTableName(String value);

    public abstract Builder setPartitionColumns(ImmutableList<String> value);

    public abstract RangeCountTransform build();
  }
}
