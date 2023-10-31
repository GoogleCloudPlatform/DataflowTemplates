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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerChangeStreamsUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * The {@link TrackedSpannerTable} contains the table name and the columns of a Spanner table
 * tracked by a change stream.
 */
@DefaultCoder(AvroCoder.class)
public final class TrackedSpannerTable implements Serializable {

  private String tableName;
  // Primary key should be exactly the same as the tracked Spanner table.
  private List<TrackedSpannerColumn> pkColumns;
  // Non-primary key only include the tracked Spanner columns.
  private List<TrackedSpannerColumn> nonPkColumns;
  private List<TrackedSpannerColumn> allColumns;

  /** Default constructor for serialization only. */
  public TrackedSpannerTable() {}

  private static class SortByPkOrdinalPosition implements Comparator<TrackedSpannerColumn> {
    public int compare(TrackedSpannerColumn o1, TrackedSpannerColumn o2) {
      return Integer.compare(o1.getPkOrdinalPosition(), o2.getPkOrdinalPosition());
    }
  }

  private static class SortByOrdinalPosition implements Comparator<TrackedSpannerColumn> {
    public int compare(TrackedSpannerColumn o1, TrackedSpannerColumn o2) {
      return Integer.compare(o1.getOrdinalPosition(), o2.getOrdinalPosition());
    }
  }

  public TrackedSpannerTable(
      String tableName,
      List<TrackedSpannerColumn> pkColumns,
      List<TrackedSpannerColumn> nonPkColumns) {
    this.pkColumns = new ArrayList<>(pkColumns);
    this.nonPkColumns = new ArrayList<>(nonPkColumns);
    // Sort the primary key column by primary key ordinal position.
    Collections.sort(this.pkColumns, new SortByPkOrdinalPosition());
    Collections.sort(this.nonPkColumns, new SortByOrdinalPosition());
    this.tableName = tableName;

    this.allColumns = new ArrayList<>();
    allColumns.addAll(this.pkColumns);
    allColumns.addAll(this.nonPkColumns);
  }

  public String getTableName() {
    return tableName;
  }

  public List<TrackedSpannerColumn> getPkColumns() {
    return pkColumns;
  }

  public List<TrackedSpannerColumn> getNonPkColumns() {
    return nonPkColumns;
  }

  public List<TrackedSpannerColumn> getAllColumns() {
    return allColumns;
  }

  // TrackedSpannerColumn.create requires name, type, ordinalPosition, pkOrdinalPosition. The
  // ordinal position of the primary key should be set to -1 for non-primary key.
  public void addTrackedSpannerNonPkColumn(ModColumnType spannerColumn) {
    TrackedSpannerColumn newSpannerColumnObj =
        TrackedSpannerColumn.create(
            spannerColumn.getName(),
            SpannerChangeStreamsUtils.informationSchemaGoogleSQLTypeToSpannerType(
                spannerColumn.getType().getCode()),
            (int) spannerColumn.getOrdinalPosition(),
            -1);
    this.nonPkColumns.add(newSpannerColumnObj);
    this.allColumns.add(newSpannerColumnObj);
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrackedSpannerTable)) {
      return false;
    }
    TrackedSpannerTable that = (TrackedSpannerTable) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(pkColumns, that.pkColumns)
        && Objects.equals(nonPkColumns, that.nonPkColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, pkColumns, nonPkColumns);
  }

  @Override
  public String toString() {
    return "TrackedSpannerTable{"
        + "tableName='"
        + tableName
        + '\''
        + ", pkColumns="
        + pkColumns
        + ", nonPkColumns="
        + nonPkColumns
        + ", allColumns="
        + allColumns
        + '}';
  }
}
