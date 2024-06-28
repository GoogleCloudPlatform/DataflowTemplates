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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

@AutoValue
public abstract class ReadWithUniformPartitions<T> extends PTransform<PBegin, PCollection<T>> {

  /* We try to get Ranges, where a Range is split if it's count is greater than twice the mean */
  public static final long SPLITTER_MAX_RELATIVE_DEVIATION = 1;

  public static final long SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS = 30 * 1000;

  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  abstract UniformSplitterDBAdapter dbAdapter();

  abstract long countQueryTimeoutMillis();

  abstract String tableName();

  abstract ImmutableList<PartitionColumn> partitionColumns();

  abstract Long approxTableCount();

  abstract Long maxPartitionsHint();

  abstract Long initialSplitHint();

  abstract Long splitStageCountHint();

  abstract JdbcIO.RowMapper<T> rowMapper();

  @Nullable
  abstract PTransform<PCollection<ImmutableList<Range>>, ?> rangesPeek();

  @Nullable
  abstract PCollection<?> waitOnSignal();

  @Nullable
  abstract Range initialRange();

  @Override
  public PCollection<T> expand(PBegin input) {
    // TODO(vardhanvthigle): Implement mapper for strings of various collations as side input.
    BoundaryTypeMapper typeMapper = null;
    // Generate Initial Ranges with liner splits (No need to do execute count queries the DB here)
    PCollection<ImmutableList<Range>> ranges = initialSplit(input, typeMapper);
    // Classify Ranges to count or get new columns and perform the operations.
    for (long i = 0; i < splitStageCountHint(); i++) {
      ranges = addStage(ranges, typeMapper, i);
    }

    ImmutableList<String> colNames =
        partitionColumns().stream()
            .map(PartitionColumn::columnName)
            .collect(ImmutableList.toImmutableList());
    PreparedStatementSetter<Range> rangePrepareator =
        new RangePreparedStatementSetter(colNames.size());

    // Merge the Ranges towards the mean.
    PCollection<ImmutableList<Range>> mergedRanges =
        ranges.apply(
            getTransformName("RangeMerge", null),
            ParDo.of(
                MergeRangesDoFn.builder()
                    .setApproxTableCount(approxTableCount())
                    .setMaxPartitionHint(maxPartitionsHint())
                    .build()));
    PCollection<Range> rangesToRead =
        peekRanges(mergedRanges).apply(ParDo.of(new UnflattenRangesDoFn()));
    return rangesToRead.apply(
        getTransformName("RangeRead", null),
        JdbcIO.<Range, T>readAll()
            .withOutputParallelization(false)
            .withQuery(dbAdapter().getReadQuery(tableName(), colNames))
            .withParameterSetter(rangePrepareator)
            .withDataSourceProviderFn(dataSourceProviderFn())
            .withRowMapper(rowMapper()));
  }

  public static <T> Builder<T> builder() {
    return new AutoValue_ReadWithUniformPartitions.Builder<T>()
        .setCountQueryTimeoutMillis(SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS);
  }

  /** Gives log to the base 2 of a given value rounded up. */
  @VisibleForTesting
  protected static long logToBaseTwo(long value) {
    return value == 0L ? 0L : 64L - Long.numberOfLeadingZeros(value - 1);
  }

  private PCollection<ImmutableList<Range>> initialSplit(
      PBegin input, @Nullable BoundaryTypeMapper typeMapper) {

    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setBoundaryTypeMapper(typeMapper)
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .build();

    RangeCountTransform rangeCountTransform =
        RangeCountTransform.builder()
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .setTimeoutMillis(countQueryTimeoutMillis())
            .build();
    ColumnForBoundaryQuery initialColumn =
        ColumnForBoundaryQuery.builder()
            .setPartitionColumn(partitionColumns().get(0))
            .setParentRange(null)
            .build();
    long splitHeight = logToBaseTwo(initialSplitHint());

    PCollection<Range> initialRange;
    if (initialRange() == null) {
      initialRange =
          wait(input.apply(Create.of(ImmutableList.of(initialColumn))))
              .apply(rangeBoundaryTransform);
    } else {
      initialRange = wait(input.apply(Create.of(ImmutableList.of(initialRange()))));
    }

    return initialRange.apply(
        getTransformName("InitialRangeSplit", null),
        ParDo.of(InitialSplitRangeDoFn.builder().setSplitHeight(splitHeight).build()));
  }

  private PCollection<ImmutableList<Range>> addStage(
      PCollection<ImmutableList<Range>> input, BoundaryTypeMapper typeMapper, long stageIdx) {

    RangeClassifierDoFn classifierFn =
        RangeClassifierDoFn.builder()
            .setApproxTableCount(approxTableCount())
            .setMaxPartitionHint(maxPartitionsHint())
            .setPartitionColumns(partitionColumns())
            .setStageIdx(stageIdx)
            .build();

    RangeCountTransform rangeCountTransform =
        RangeCountTransform.builder()
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .setTimeoutMillis(countQueryTimeoutMillis())
            .build();

    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setBoundaryTypeMapper(typeMapper)
            .setDbAdapter(dbAdapter())
            .setPartitionColumns(
                partitionColumns().stream()
                    .map(c -> c.columnName())
                    .collect(ImmutableList.toImmutableList()))
            .setTableName(tableName())
            .build();
    PCollectionTuple classifiedRanges =
        input.apply(
            getTransformName("RangeClassifier", stageIdx),
            ParDo.of(classifierFn)
                .withOutputTags(
                    RangeClassifierDoFn.TO_COUNT_TAG,
                    TupleTagList.of(RangeClassifierDoFn.TO_ADD_COLUMN_TAG)
                        .and(RangeClassifierDoFn.TO_RETAIN_TAG)));
    PCollection<Range> countedRanges =
        classifiedRanges
            .get(RangeClassifierDoFn.TO_COUNT_TAG)
            .apply(getTransformName("RangeCounter", stageIdx), rangeCountTransform);
    PCollection<ColumnForBoundaryQuery> rangesToAddColumn =
        classifiedRanges.get(RangeClassifierDoFn.TO_ADD_COLUMN_TAG);
    PCollection<Range> rangesWithNewColumns =
        rangesToAddColumn
            .apply(getTransformName("RangeBoundary", stageIdx), rangeBoundaryTransform)
            .apply(
                getTransformName("RangeSplitterAfterColumnAddition", stageIdx),
                ParDo.of(new SplitRangeDoFn()))
            .apply(getTransformName("CountAfterColumnAddition", stageIdx), rangeCountTransform);
    PCollection<Range> retainedRanges = classifiedRanges.get(RangeClassifierDoFn.TO_RETAIN_TAG);

    return PCollectionList.of(retainedRanges)
        .and(countedRanges)
        .and(rangesWithNewColumns)
        .apply(getTransformName("FlattenProcessedRanges", stageIdx), Flatten.pCollections())
        .apply(getTransformName("RangeCombiner", stageIdx), RangeCombiner.globally());
  }

  private String getTransformName(String transformType, @Nullable Long stageIdx) {
    String name = "Table." + tableName() + "." + transformType;
    if (stageIdx != null) {
      name = name + "." + stageIdx;
    }
    return name;
  }

  private <V extends PCollection> V wait(V input) {
    // TOD (vardhanbvthigle): Take a list.
    if (waitOnSignal() == null) {
      return input;
    } else {
      return (V) input.apply(Wait.on(waitOnSignal()));
    }
  }

  private PCollection<ImmutableList<Range>> peekRanges(
      PCollection<ImmutableList<Range>> mergedRanges) {
    if (rangesPeek() == null) {
      return mergedRanges;
    } else {
      return mergedRanges.apply(Wait.on((PCollection<?>) mergedRanges.apply(rangesPeek())));
    }
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> setDataSourceProviderFn(
        SerializableFunction<Void, DataSource> value);

    public abstract Builder<T> setDbAdapter(UniformSplitterDBAdapter value);

    public abstract Builder<T> setCountQueryTimeoutMillis(long value);

    public abstract Builder<T> setTableName(String value);

    public abstract Builder<T> setPartitionColumns(ImmutableList<PartitionColumn> value);

    abstract ImmutableList<PartitionColumn> partitionColumns();

    public abstract Builder<T> setApproxTableCount(Long value);

    abstract Long approxTableCount();

    public abstract Builder<T> setMaxPartitionsHint(Long value);

    abstract Optional<Long> maxPartitionsHint();

    public abstract Builder<T> setInitialSplitHint(Long value);

    abstract Optional<Long> initialSplitHint();

    public abstract Builder<T> setSplitStageCountHint(Long value);

    public abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> value);

    public abstract Builder<T> setRangesPeek(
        @Nullable PTransform<PCollection<ImmutableList<Range>>, ?> value);

    public abstract Builder<T> setWaitOnSignal(@Nullable PCollection<?> value);

    public abstract Builder<T> setInitialRange(Range value);

    abstract Optional<Long> splitStageCountHint();

    abstract ReadWithUniformPartitions<T> autoBuild();

    public ReadWithUniformPartitions<T> build() {
      long maxPartitionsHint;
      if (maxPartitionsHint().isPresent()) {
        maxPartitionsHint = maxPartitionsHint().get();
      } else {
        maxPartitionsHint = Math.max(1, Math.round(Math.floor(Math.sqrt(approxTableCount()) / 10)));
        setMaxPartitionsHint(maxPartitionsHint);
      }
      if (!initialSplitHint().isPresent()) {
        setInitialSplitHint(SPLITTER_MAX_RELATIVE_DEVIATION * maxPartitionsHint);
      }
      if (!splitStageCountHint().isPresent()) {
        Long splitHeightHint =
            logToBaseTwo(maxPartitionsHint)
                + partitionColumns().size()
                + 1 /* For initial counts */;
        setSplitStageCountHint(splitHeightHint);
      }
      return autoBuild();
    }
  }
}
