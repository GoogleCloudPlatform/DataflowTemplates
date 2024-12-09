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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.BoundaryTypeMapperImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadAll;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadWithUniformPartitions<T> extends PTransform<PBegin, PCollection<T>> {

  private static final Logger logger = LoggerFactory.getLogger(ReadWithUniformPartitions.class);

  /* We try to get Ranges, where a Range is split if it's count is greater than twice the mean */
  /* TODO(vardhanvthigle): Add this as a configurable parameter */
  public static final long SPLITTER_MAX_RELATIVE_DEVIATION = 1;
  private static final long SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS = 5 * 1000;

  /**
   * Auto Inference of max partitions similar to <a
   * href=https://github.com/apache/beam/blob/b50ad0fe8fc168eaded62efb08f19cf2aea341e2/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L1398>JDBCIO#readWithPartitions</a>
   * We scale the square root of partitions to 20 instead of 10 as in the initial split we begin the
   * closest power of 2 on the larger side.
   */
  private static final int MAX_PARTITION_INFERENCE_SCALE_FACTOR = 20;

  /** Provider for {@link DataSource}. Required parameter. */
  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  /**
   * Implementations of {@link UniformSplitterDBAdapter} to get queries as per the dialect of the
   * database. Required parameter.
   */
  abstract UniformSplitterDBAdapter dbAdapter();

  /** Name of the table to read. Required parameter. */
  abstract String tableName();

  /** List of partition columns. Required parameter. */
  abstract ImmutableList<PartitionColumn> partitionColumns();

  /**
   * Approximate count of rows in the table. Required parameter. The caller can set this as per the
   * cardinality of primary key index, or derived from information schema tables. This is used to
   * auto infer {@link ReadWithUniformPartitions#maxPartitionsHint()} and hence the count for
   * initial split of ranges. As soon as all the ranges get counts (there is no time out for
   * counting the size of a range), {@link ReadWithUniformPartitions} is able to get the total count
   * of rows of the table naturally. If {@link ReadWithUniformPartitions#autoAdjustMaxPartitions()}
   * is set to true (which is the default), the approximate count can be within half to double of
   * the actual count still giving the expected split.
   */
  // TODO(vardhanvthigle:) make this optional by auto inference.
  abstract Long approxTotalRowCount();

  /**
   * Row mapper to map the {@link java.sql.ResultSet}.
   *
   * @see org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper RowMapper.
   */
  abstract JdbcIO.RowMapper<T> rowMapper();

  /**
   * Max fetch size for jdbc read. * If Null, {@link JdbcIO JdbcIO's} default fetch size of 50_000
   * gets used. {@link JdbcIO.Read#withFetchSize(int)} recommends setting this manually only if the
   * default value gives out of memory errors.
   */
  @Nullable
  abstract Integer fetchSize();

  /**
   * Hint for Maximum number of partitions of the source key space. If not set, it is auto inferred
   * as 1/10 * sqrt({@link ReadWithUniformPartitions#autoAdjustMaxPartitions()}). Note that if
   * {@link ReadWithUniformPartitions#autoAdjustMaxPartitions()} is set to true, the aggregated
   * count of all ranges will auto-adjust this value by replacing the approximate count in above
   * expression.
   */
  abstract Long maxPartitionsHint();

  /**
   * If set to true, the aggregated count of all ranges will auto-adjust {@link
   * ReadWithUniformPartitions#maxPartitionsHint()}.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /**
   * Timeout of the count query in milliseconds. Defaults to {@link
   * ReadWithUniformPartitions#SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS}
   */
  abstract long countQueryTimeoutMillis();

  /**
   * Hint for number of initial split of ranges. Defaults to {@link
   * ReadWithUniformPartitions#maxPartitionsHint()}.
   */
  abstract Long initialSplitHint();

  /**
   * Number of stages for the splitting process. This can be set to smaller values like 2/3 for auto
   * incrementing keys. Defaults to log2({@link ReadWithUniformPartitions#maxPartitionsHint()}).
   */
  abstract Long splitStageCountHint();

  /**
   * If not null, limits the maximum number of parallel operations queued on a DB per transform.
   * Defaults to null. It's best to set this to a number close to number of cores available on mySql
   * server.
   */
  @Nullable
  abstract Integer dbParallelizationForSplitProcess();

  /**
   * If not null, limits the maximum number of parallel operations queued on a DB per transform.
   * Defaults to null.
   */
  @Nullable
  abstract Integer dbParallelizationForReads();

  /**
   * An optional transform that can be injected at the end of splitting process to make use of the
   * ranges. This could range anywhere for logging to gcs, to creating split points on spanner, to
   * even ease of unit testing.
   */
  @Nullable
  abstract PTransform<PCollection<ImmutableList<Range>>, ?> additionalOperationsOnRanges();

  /**
   * Wait for completion of dependencies as per the requirements of the pipeline. This wait is
   * applied before the first query is made to the database to detect split points.
   */
  @Nullable
  abstract OnSignal<?> waitOn();

  /**
   * An optional, initial range to start with. This can help anywhere from unit testing, to micro
   * batching (read only columns after a timestamp), to splitting the whole split detection and read
   * of a really large table.
   *
   * @return
   */
  @Nullable
  abstract Range initialRange();

  @Override
  public PCollection<T> expand(PBegin input) {
    // TODO(vardhanvthigle): Move this side-input generation out to DB level.
    PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView =
        getCollationMapperView(input);
    BoundaryTypeMapper typeMapper =
        BoundaryTypeMapperImpl.builder().setCollationMapperView(collationMapperView).build();
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
                        .setApproxTotalRowCount(approxTotalRowCount())
                        .setMaxPartitionHint(maxPartitionsHint())
                        .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
                        .setTableName(tableName())
                        .build())
                .withSideInputs(typeMapper.getCollationMapperView()));
    PCollection<Range> rangesToRead =
        peekRanges(mergedRanges)
            .apply(
                ParDo.of(new UnflattenRangesDoFn())
                    .withSideInputs(typeMapper.getCollationMapperView()));
    return rangesToRead
        .apply(
            getTransformName("ReshuffleFinal", null),
            Reshuffle.<Range>viaRandomKey().withNumBuckets(dbParallelizationForReads()))
        .apply(
            getTransformName("RangeRead", null),
            buildJdbcIO(
                JdbcIO.<Range, T>readAll(),
                dbAdapter().getReadQuery(tableName(), colNames),
                rangePrepareator,
                dataSourceProviderFn(),
                rowMapper(),
                fetchSize()));
  }

  @VisibleForTesting
  protected static <T> JdbcIO.ReadAll<Range, T> buildJdbcIO(
      JdbcIO.ReadAll<Range, T> readAll,
      String readQuery,
      PreparedStatementSetter<Range> rangePrepareator,
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      RowMapper<T> rowMapper,
      Integer fetchSize) {
    ReadAll<Range, T> ret =
        readAll
            .withOutputParallelization(false)
            .withQuery(readQuery)
            .withParameterSetter(rangePrepareator)
            .withDataSourceProviderFn(dataSourceProviderFn)
            .withRowMapper(rowMapper);
    if (fetchSize != null) {
      ret = ret.withFetchSize(fetchSize);
    }
    return ret;
  }

  public static <T> Builder<T> builder() {
    return new AutoValue_ReadWithUniformPartitions.Builder<T>()
        .setCountQueryTimeoutMillis(SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS)
        .setDbParallelizationForSplitProcess(null)
        .setDbParallelizationForReads(null)
        .setFetchSize(null)
        .setAutoAdjustMaxPartitions(true);
  }

  /** Gives log to the base 2 of a given value rounded up. */
  @VisibleForTesting
  protected static long logToBaseTwo(long value) {
    return value == 0L ? 0L : 64L - Long.numberOfLeadingZeros(value - 1);
  }

  private PCollectionView<Map<CollationReference, CollationMapper>> getCollationMapperView(
      PBegin input) {
    ImmutableList<CollationReference> collationReferences =
        this.partitionColumns().stream()
            .filter(c -> c.stringCollation() != null)
            .map(PartitionColumn::stringCollation)
            .collect(ImmutableList.toImmutableList());
    return input.apply(
        CollationMapperTransform.builder()
            .setCollationReferences(collationReferences)
            .setDbAdapter(dbAdapter())
            .setDataSourceProviderFn(dataSourceProviderFn())
            .build());
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

    long splitHeight = logToBaseTwo(initialSplitHint());

    PCollection<Range> initialRange;
    if (initialRange() == null) {
      ColumnForBoundaryQuery initialColumn =
          ColumnForBoundaryQuery.builder()
              .setPartitionColumn(partitionColumns().get(0))
              .setParentRange(null)
              .build();
      initialRange =
          wait(input.apply(Create.of(ImmutableList.of(initialColumn))))
              .apply(rangeBoundaryTransform);
    } else {
      initialRange = wait(input.apply(Create.of(ImmutableList.of(initialRange()))));
    }

    return initialRange.apply(
        getTransformName("InitialRangeSplit", null),
        ParDo.of(
                InitialSplitRangeDoFn.builder()
                    .setSplitHeight(splitHeight)
                    .setTableName(tableName())
                    .build())
            .withSideInputs(typeMapper.getCollationMapperView()));
  }

  private PCollection<ImmutableList<Range>> addStage(
      PCollection<ImmutableList<Range>> input, BoundaryTypeMapper typeMapper, long stageIdx) {

    RangeClassifierDoFn classifierFn =
        RangeClassifierDoFn.builder()
            .setApproxTotalRowCount(approxTotalRowCount())
            .setMaxPartitionHint(maxPartitionsHint())
            .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
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
            .setBoundaryTypeMapper(typeMapper)
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
                        .and(RangeClassifierDoFn.TO_RETAIN_TAG))
                .withSideInputs(typeMapper.getCollationMapperView()));
    PCollection<Range> countedRanges =
        classifiedRanges
            .get(RangeClassifierDoFn.TO_COUNT_TAG)
            .apply(
                getTransformName("ReshuffleToCount", stageIdx),
                Reshuffle.<Range>viaRandomKey().withNumBuckets(dbParallelizationForSplitProcess()))
            .apply(getTransformName("RangeCounter", stageIdx), rangeCountTransform);
    PCollection<ColumnForBoundaryQuery> rangesToAddColumn =
        classifiedRanges.get(RangeClassifierDoFn.TO_ADD_COLUMN_TAG);
    PCollection<Range> rangesWithNewColumns =
        rangesToAddColumn
            .apply(
                getTransformName("ReshuffleToAddColumn", stageIdx),
                Reshuffle.<ColumnForBoundaryQuery>viaRandomKey()
                    .withNumBuckets(dbParallelizationForSplitProcess()))
            .apply(getTransformName("RangeBoundary", stageIdx), rangeBoundaryTransform)
            .apply(
                getTransformName("RangeSplitterAfterColumnAddition", stageIdx),
                ParDo.of(new SplitRangeDoFn()).withSideInputs(typeMapper.getCollationMapperView()))
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
    if (waitOn() == null) {
      return input;
    } else {
      return (V) input.apply(waitOn());
    }
  }

  private PCollection<ImmutableList<Range>> peekRanges(
      PCollection<ImmutableList<Range>> mergedRanges) {
    if (additionalOperationsOnRanges() == null) {
      return mergedRanges;
    } else {
      return mergedRanges.apply(
          Wait.on((PCollection<?>) mergedRanges.apply(additionalOperationsOnRanges())));
    }
  }

  /**
   * Auto Inference of max partitions similar to <a
   * href=https://github.com/apache/beam/blob/b50ad0fe8fc168eaded62efb08f19cf2aea341e2/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L1398>JDBCIO#readWithPartitions</a>
   * We scale the square root of partitions to 20 instead of 10 as in the initial split we begin the
   * closest power of 2 on the larger side.
   *
   * @param count approximate count of rows to migrate.
   * @return inferred max partitions
   */
  public static long inferMaxPartitions(long count) {
    return Math.max(
        1, Math.round(Math.floor(Math.sqrt(count) / MAX_PARTITION_INFERENCE_SCALE_FACTOR)));
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

    public abstract Builder<T> setApproxTotalRowCount(Long value);

    abstract Long approxTotalRowCount();

    public abstract Builder<T> setMaxPartitionsHint(Long value);

    public abstract Builder<T> setAutoAdjustMaxPartitions(Boolean value);

    abstract Optional<Long> maxPartitionsHint();

    public abstract Builder<T> setInitialSplitHint(Long value);

    abstract Optional<Long> initialSplitHint();

    public abstract Builder<T> setSplitStageCountHint(Long value);

    public abstract Builder<T> setDbParallelizationForSplitProcess(@Nullable Integer value);

    public abstract Builder<T> setDbParallelizationForReads(@Nullable Integer value);

    public abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> value);

    public abstract Builder<T> setFetchSize(@Nullable Integer value);

    public abstract Builder<T> setAdditionalOperationsOnRanges(
        @Nullable PTransform<PCollection<ImmutableList<Range>>, ?> value);

    public abstract Builder<T> setWaitOn(@Nullable OnSignal<?> value);

    public abstract Builder<T> setInitialRange(Range value);

    @Nullable
    abstract Range initialRange();

    abstract Optional<Long> splitStageCountHint();

    abstract ReadWithUniformPartitions<T> autoBuild();

    public ReadWithUniformPartitions<T> build() {
      /* TODO(vardhanvthigle): USE `Explain Select *` to auto infer approxCount within the uniform partition splitter itself.
         Since that will be executed in construction path, it needs to be retried with backoff.
         For the context of the larger reader, the index cardinality gives a pretty good idea of the approxCount of the table too.
      */
      long maxPartitionsHint;
      if (maxPartitionsHint().isPresent()) {
        maxPartitionsHint = maxPartitionsHint().get();
      } else {
        maxPartitionsHint = inferMaxPartitions(approxTotalRowCount());
        setMaxPartitionsHint(maxPartitionsHint);
      }
      if (!initialSplitHint().isPresent()) {
        setInitialSplitHint(SPLITTER_MAX_RELATIVE_DEVIATION * maxPartitionsHint);
      }
      /* TODO(vardhanvthigle): currently every stage does a single split of a rnage that crosses the 2*Mean mark.
       * This needed not be the case. Based on the splitStageCount, we could decide how many splits to do in a range.
       * Current benchmarking suggests that reducing stages is better from overhead pov.
       */
      if (!splitStageCountHint().isPresent()) {
        Long splitHeightHint =
            logToBaseTwo(maxPartitionsHint)
                + partitionColumns().size()
                + 1 /* For initial counts */;
        setSplitStageCountHint(splitHeightHint);
      }

      if (initialRange() != null) {
        Range curRange = initialRange();
        for (PartitionColumn col : partitionColumns()) {
          Preconditions.checkState(curRange.colName().equals(col.columnName()));
          if (curRange.hasChildRange()) {
            curRange = curRange.childRange();
          } else {
            break;
          }
        }
      }
      ReadWithUniformPartitions readWithUniformPartitions = autoBuild();
      logger.info("Initialized ReadWithUniformPartitions {}", readWithUniformPartitions);
      return readWithUniformPartitions;
    }
  }
}
