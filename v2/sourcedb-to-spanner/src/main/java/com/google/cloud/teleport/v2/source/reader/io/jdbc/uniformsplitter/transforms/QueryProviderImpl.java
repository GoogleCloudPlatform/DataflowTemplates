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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.MultiTableReadAll.QueryProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link QueryProvider} that maintains a mapping from {@link TableIdentifier} to
 * the appropriate SQL read query.
 *
 * <p>This allows the {@link MultiTableReadFn} to dynamically look up the correct query for any
 * given {@link Range} during the data extraction phase.
 */
@AutoValue
public abstract class QueryProviderImpl implements QueryProvider<Range> {
  private static final Logger logger = LoggerFactory.getLogger(QueryProviderImpl.class);

  abstract ImmutableMap<TableIdentifier, String> queryMap();

  @Override
  public String getQuery(Range element) throws Exception {
    if (!queryMap().containsKey(element.tableIdentifier())) {

      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {} and {}",
          element,
          queryMap());
      throw new RuntimeException("Invalid Range");
    }
    return queryMap().get(element.tableIdentifier());
  }

  public static Builder builder() {
    return new AutoValue_QueryProviderImpl.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract ImmutableMap.Builder<TableIdentifier, String> queryMapBuilder();

    public Builder setTableSplitSpecifications(
        Iterable<TableSplitSpecification> tableSplitSpecifications,
        UniformSplitterDBAdapter dbAdapter) {
      for (TableSplitSpecification tableSplitSpecification : tableSplitSpecifications) {
        String query =
            dbAdapter.getReadQuery(
                tableSplitSpecification.tableIdentifier().tableName(),
                tableSplitSpecification.partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()));
        queryMapBuilder().put(tableSplitSpecification.tableIdentifier(), query);
      }
      return this;
    }

    public abstract QueryProviderImpl build();
  }
}
