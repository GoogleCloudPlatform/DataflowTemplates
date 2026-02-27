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
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.BoundaryTypeMapperImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.ReadAll;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.Wait.OnSignal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that reads from one or more database tables using uniform partitions.
 *
 * <p>This transform refactors the traditional single-table uniform splitter to support multiple
 * tables within a single Dataflow job while maintaining a <b>constant-size Dataflow graph</b>. By
 * lifting table-specific logic out of the graph construction and into data-driven {@link DoFn}s, it
 * enables scaling to thousands of tables (e.g., 5,000+) without hitting Dataflow graph size limits.
 *
 * <p>The process consists of:
 *
 * <ol>
 *   <li><b>Initial Split</b>: Creating starting ranges for all tables.
 *   <li><b>Iterative Splitting</b>: A chained sequence of stages where ranges are counted,
 *       partitioned, and refined.
 *   <li><b>Graduation Lane</b>: An optimization that allows tables reaching their accuracy goals to
 *       "graduate" early from the loop, preventing stragglers from blocking the entire pipeline.
 *   <li><b>Merge and Read</b>: Final ranges are combined, merged toward a target mean, and read in
 *       parallel using a generic multi-table reader.
 * </ol>
 *
 * @param <T> the type of elements to be read.
 */
@AutoValue
public abstract class ReadWithUniformPartitions<T> extends PTransform<PBegin, PCollection<T>> {

  private static final Logger logger = LoggerFactory.getLogger(ReadWithUniformPartitions.class);

  private static final long SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS = 5 * 1000;

  /** Provider for {@link DataSource}. Required parameter. */
  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  /**
   * Implementations of {@link UniformSplitterDBAdapter} to get queries as per the dialect of the
   * database. Required parameter.
   */
  abstract UniformSplitterDBAdapter dbAdapter();

  /** Specification for splitting the table. Required parameter. */
  abstract ImmutableList<TableSplitSpecification> tableSplitSpecifications();

  /** Specification for reading from the table. Required parameter. */
  abstract ImmutableMap<TableIdentifier, TableReadSpecification<T>> tableReadSpecifications();

  @Memoized
  long maxSplitStages() {
    return tableSplitSpecifications().stream()
        .mapToLong(TableSplitSpecification::splitStagesCount)
        .max()
        .orElse(0L);
  }

  /**
   * If set to true, the aggregated count of all ranges will auto-adjust {@link
   * TableSplitSpecification#maxPartitionsHint()}.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /**
   * Timeout of the count query in milliseconds. Defaults to {@link
   * ReadWithUniformPartitions#SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS}
   */
  abstract long countQueryTimeoutMillis();

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
  abstract PTransform<PCollection<KV<Integer, ImmutableList<Range>>>, ?>
      additionalOperationsOnRanges();

  /**
   * Wait for completion of dependencies as per the requirements of the pipeline. This wait is
   * applied before the first query is made to the database to detect split points.
   */
  @Nullable
  abstract OnSignal<?> waitOn();

  /**
   * Number of batches for the batched range combiner. Defaults to 100. See {@link
   * RangeCombiner#perKeyBatched(int)} for details.
   */
  abstract long numBatches();

  @Nullable
  abstract String transformPrefix();

  /**
   * Orchestrates the multi-table partitioning and reading process.
   *
   * @param input the starting point for the pipeline.
   * @return a {@link PCollection} containing the data read from all configured tables.
   */
  @Override
  public PCollection<T> expand(PBegin input) {
    // TODO(vardhanvthigle): Move this side-input generation out to DB level.
    PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView =
        getCollationMapperView(input);
    BoundaryTypeMapper typeMapper =
        BoundaryTypeMapperImpl.builder().setCollationMapperView(collationMapperView).build();

    // Generate Initial Ranges with liner splits (No need to do execute count queries the DB here)
    PCollection<Range> rangesToProcess =
        initialSplit(input, typeMapper)
            .apply(
                getTransformName("UnKeyInitialRanges", null, null), ParDo.of(new UnKeyRangesDoFn()))
            .apply(
                getTransformName("UnflattenForLoop", null, null),
                ParDo.of(new UnflattenRangesDoFn())
                    .withSideInputs(typeMapper.getCollationMapperView()));
    PCollectionList<Range> graduatedRanges = PCollectionList.empty(input.getPipeline());

    for (long i = 0; i < maxSplitStages(); i++) {
      // Re-key before classifier
      PCollection<KV<Integer, ImmutableList<Range>>> keyedRanges =
          rangesToProcess.apply(
              getTransformName("CombinePreClassifier", null, i),
              RangeCombiner.perKeyBatched((int) numBatches()));

      RangeClassifierDoFn classifierFn =
          RangeClassifierDoFn.builder()
              .setTableSplitSpecifications(tableSplitSpecifications())
              .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
              .setStageIdx(i)
              .build();

      RangeCountTransform rangeCountTransform =
          RangeCountTransform.builder()
              .setDataSourceProviderFn(dataSourceProviderFn())
              .setDbAdapter(dbAdapter())
              .setTableSplitSpecifications(tableSplitSpecifications())
              .setBoundaryTypeMapper(typeMapper)
              .setTimeoutMillis(countQueryTimeoutMillis())
              .build();

      RangeBoundaryTransform rangeBoundaryTransform =
          RangeBoundaryTransform.builder()
              .setDataSourceProviderFn(dataSourceProviderFn())
              .setBoundaryTypeMapper(typeMapper)
              .setDbAdapter(dbAdapter())
              .setTableSplitSpecifications(tableSplitSpecifications())
              .build();

      PCollectionTuple classifiedRanges =
          keyedRanges.apply(
              getTransformName("RangeClassifier", null, i),
              ParDo.of(classifierFn)
                  .withOutputTags(
                      RangeClassifierDoFn.TO_COUNT_TAG,
                      TupleTagList.of(RangeClassifierDoFn.TO_ADD_COLUMN_TAG)
                          .and(RangeClassifierDoFn.TO_RETAIN_TAG)
                          .and(RangeClassifierDoFn.READY_TO_READ_TAG))
                  .withSideInputs(typeMapper.getCollationMapperView()));

      // Collect graduated ranges
      graduatedRanges =
          graduatedRanges.and(classifiedRanges.get(RangeClassifierDoFn.READY_TO_READ_TAG));

      // Process other tags
      PCollection<Range> countedRanges =
          classifiedRanges
              .get(RangeClassifierDoFn.TO_COUNT_TAG)
              .apply(
                  getTransformName("ReshuffleToCount", null, i),
                  Reshuffle.<Range>viaRandomKey()
                      .withNumBuckets(dbParallelizationForSplitProcess()))
              .apply(getTransformName("RangeCounter", null, i), rangeCountTransform);
      PCollection<ColumnForBoundaryQuery> rangesToAddColumn =
          classifiedRanges.get(RangeClassifierDoFn.TO_ADD_COLUMN_TAG);
      PCollection<Range> rangesWithNewColumns =
          rangesToAddColumn
              .apply(
                  getTransformName("ReshuffleToAddColumn", null, i),
                  Reshuffle.<ColumnForBoundaryQuery>viaRandomKey()
                      .withNumBuckets(dbParallelizationForSplitProcess()))
              .apply(getTransformName("RangeBoundary", null, i), rangeBoundaryTransform)
              .apply(
                  getTransformName("RangeSplitterAfterColumnAddition", null, i),
                  ParDo.of(new SplitRangeDoFn())
                      .withSideInputs(typeMapper.getCollationMapperView()))
              .apply(getTransformName("CountAfterColumnAddition", null, i), rangeCountTransform);
      PCollection<Range> retainedRanges = classifiedRanges.get(RangeClassifierDoFn.TO_RETAIN_TAG);

      // Assemble input for next iteration
      rangesToProcess =
          PCollectionList.of(retainedRanges)
              .and(countedRanges)
              .and(rangesWithNewColumns)
              .apply(getTransformName("FlattenProcessedRanges", null, i), Flatten.pCollections());
    }

    // Add remaining ranges from the loop to the graduated list
    graduatedRanges = graduatedRanges.and(rangesToProcess);

    PCollection<KV<Integer, ImmutableList<Range>>> allRangesCombined =
        graduatedRanges
            .apply(getTransformName("FlattenGraduatedRanges", null, null), Flatten.pCollections())
            .apply(
                getTransformName("CombineFinalRanges", null, null),
                RangeCombiner.perKeyBatched((int) numBatches()));

    // 1. Merge the Ranges towards the mean.
    PCollection<KV<Integer, ImmutableList<Range>>> mergedRanges =
        allRangesCombined.apply(
            getTransformName("MergeRanges", null, null),
            ParDo.of(
                MergeRangesDoFn.builder()
                    .setTableSplitSpecifications(tableSplitSpecifications())
                    .setAutoAdjustMaxPartitions(autoAdjustMaxPartitions())
                    .build()));

    // 2. Apply optional "peek" operation for testing/debugging.
    PCollection<?> peekSignal = peekRanges(mergedRanges);

    // 3. Prepare for read: Wait for peek, Un-Key, and Unflatten.
    // We wait for the peek operation (e.g. spanner split point creation) to finish
    // before starting the massive parallel read.
    PCollection<Range> rangesToRead =
        mergedRanges
            .apply(
                getTransformName("WaitOn", null, null),
                Wait.on(peekSignal)) // Wait for the peek operation to finish
            .apply(getTransformName("UnKeyRanges", null, null), ParDo.of(new UnKeyRangesDoFn()))
            .apply(
                getTransformName("UnflattenRangesForRead", null, null),
                ParDo.of(new UnflattenRangesDoFn())
                    .withSideInputs(typeMapper.getCollationMapperView()));

    PreparedStatementSetter<Range> rangePrepareator =
        new RangePreparedStatementSetter(tableSplitSpecifications());

    // 4. Final Read operation
    // We reshuffle before the read to ensure that ranges from thousands of tables
    // are distributed across all available workers, preventing any single worker
    // from becoming a bottleneck.
    return rangesToRead
        .apply(
            getTransformName("ReshuffleFinal", null, null),
            Reshuffle.<Range>viaRandomKey().withNumBuckets(dbParallelizationForReads()))
        .apply(
            getTransformName("RangeRead", null, null),
            buildMultiTableRead(
                MultiTableReadAll.builder(),
                tableSplitSpecifications(),
                tableReadSpecifications(),
                dbAdapter(),
                rangePrepareator,
                dataSourceProviderFn()));
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

  /**
   * Internal helper to build the {@link MultiTableReadAll} transform.
   *
   * @param readAllBuilder the builder for MultiTableReadAll.
   * @param tableSplitSpecifications specifications for splitting tables.
   * @param tableReadSpecifications specifications for reading tables.
   * @param dbAdapter the database adapter for generating queries.
   * @param rangePrepareator the parameter setter for the read query.
   * @param dataSourceProviderFn the provider for the data source.
   * @return a configured MultiTableReadAll transform.
   */
  @VisibleForTesting
  protected static <T> MultiTableReadAll<Range, T> buildMultiTableRead(
      MultiTableReadAll.Builder readAllBuilder,
      ImmutableList<TableSplitSpecification> tableSplitSpecifications,
      ImmutableMap<TableIdentifier, TableReadSpecification<T>> tableReadSpecifications,
      UniformSplitterDBAdapter dbAdapter,
      PreparedStatementSetter<Range> rangePrepareator,
      SerializableFunction<Void, DataSource> dataSourceProviderFn) {
    QueryProviderImpl queryProvider =
        QueryProviderImpl.builder()
            .setTableSplitSpecifications(tableSplitSpecifications, dbAdapter)
            .build();
    MultiTableReadAll<Range, T> ret =
        readAllBuilder
            .setOutputParallelization(false)
            .setQueryProvider(StaticValueProvider.of(queryProvider))
            .setParameterSetter(rangePrepareator)
            .setDataSourceProviderFn(dataSourceProviderFn)
            .setTableReadSpecifications(tableReadSpecifications)
            .setTableIdentifierFn(new RangeToTableIdentifierFn())
            .setDisableAutoCommit(true)
            .build();
    return ret;
  }

  public static <T> Builder<T> builder() {
    return new AutoValue_ReadWithUniformPartitions.Builder<T>()
        .setCountQueryTimeoutMillis(SPLITTER_DEFAULT_COUNT_QUERY_TIMEOUT_MILLIS)
        .setDbParallelizationForSplitProcess(null)
        .setDbParallelizationForReads(null)
        .setAutoAdjustMaxPartitions(true)
        .setNumBatches(100L);
  }

  private PCollectionView<Map<CollationReference, CollationMapper>> getCollationMapperView(
      PBegin input) {
    ImmutableList<CollationReference> collationReferences =
        getCollationReferences(this.tableSplitSpecifications());
    return input.apply(
        getTransformName("CollationMapper", null, null),
        CollationMapperTransform.builder()
            .setCollationReferences(collationReferences)
            .setDbAdapter(dbAdapter())
            .setDataSourceProviderFn(dataSourceProviderFn())
            .build());
  }

  /**
   * Extracts and deduplicates all unique collation references from a list of table split
   * specifications. This is used to prepare for the {@link CollationMapperTransform} to ensure that
   * the database is queried for a given collation only once, even if it is used across multiple
   * columns or tables.
   *
   * @param tableSplitSpecifications A list of {@link TableSplitSpecification} objects.
   * @return An immutable list of unique {@link CollationReference} objects.
   */
  @VisibleForTesting
  protected static ImmutableList<CollationReference> getCollationReferences(
      ImmutableList<TableSplitSpecification> tableSplitSpecifications) {
    ImmutableSet<CollationReference> collationReferences =
        tableSplitSpecifications.stream()
            .flatMap(t -> t.partitionColumns().stream())
            .filter(c -> c.stringCollation() != null)
            .map(PartitionColumn::stringCollation)
            .collect(ImmutableSet.toImmutableSet());
    return ImmutableList.copyOf(collationReferences);
  }

  private PCollection<KV<Integer, ImmutableList<Range>>> initialSplit(
      PBegin input, @Nullable BoundaryTypeMapper typeMapper) {

    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setBoundaryTypeMapper(typeMapper)
            .setDataSourceProviderFn(dataSourceProviderFn())
            .setDbAdapter(dbAdapter())
            .setTableSplitSpecifications(tableSplitSpecifications())
            .build();

    PCollection<Range> initialRanges;
    ImmutableList<ColumnForBoundaryQuery> initialColumns =
        tableSplitSpecifications().stream()
            .filter(spec -> spec.initialRange() == null)
            .map(
                spec ->
                    ColumnForBoundaryQuery.builder()
                        .setTableIdentifier(spec.tableIdentifier())
                        .setPartitionColumn(spec.partitionColumns().get(0))
                        .setParentRange(null)
                        .build())
            .collect(ImmutableList.toImmutableList());

    PCollectionList<Range> initialRangesList = PCollectionList.empty(input.getPipeline());

    if (!initialColumns.isEmpty()) {
      initialRangesList =
          initialRangesList.and(
              wait(
                      input.apply(
                          getTransformName("InitialColumns", null, null),
                          Create.of(initialColumns)),
                      "InitialColumns")
                  .apply(rangeBoundaryTransform));
    }

    ImmutableList<Range> preConfiguredInitialRanges =
        tableSplitSpecifications().stream()
            .filter(spec -> spec.initialRange() != null)
            .map(TableSplitSpecification::initialRange)
            .collect(ImmutableList.toImmutableList());

    if (!preConfiguredInitialRanges.isEmpty()) {
      initialRangesList =
          initialRangesList.and(
              wait(
                  input.apply(
                      getTransformName("InitialRanges", null, null),
                      Create.of(preConfiguredInitialRanges)),
                  "InitialRanges"));
    }

    initialRanges =
        initialRangesList.apply(
            getTransformName("FlattenInitialRanges", null, null), Flatten.pCollections());

    PCollection<ImmutableList<Range>> splitRangesAsList =
        initialRanges.apply(
            getTransformName("InitialRangeSplit", null, null),
            ParDo.of(
                    InitialSplitRangeDoFn.builder()
                        .setTableSplitSpecifications(tableSplitSpecifications())
                        .build())
                .withSideInputs(typeMapper.getCollationMapperView()));

    PCollection<Range> splitRanges =
        splitRangesAsList.apply(
            getTransformName("UnflattenInitialSplit", null, null),
            ParDo.of(new UnflattenRangesDoFn())
                .withSideInputs(typeMapper.getCollationMapperView()));

    return splitRanges.apply(
        getTransformName("InitialCombine", null, null),
        RangeCombiner.perKeyBatched((int) numBatches()));
  }

  /**
   * Generates a unique name for a transform within the multi-table pipeline.
   *
   * @param transformType the type of transform (e.g., "RangeClassifier").
   * @param tableName optional table name to include in the prefix.
   * @param stageIdx optional stage index for iterative transforms.
   * @return a unique transform name.
   */
  private String getTransformName(
      String transformType, @Nullable String tableName, @Nullable Long stageIdx) {
    String name = transformPrefix() + ".";
    if (tableName != null) {
      name += "Table." + tableName + ".";
    }
    name += transformType;
    if (stageIdx != null) {
      name = name + "." + stageIdx;
    }
    return name;
  }

  /**
   * Applies an optional wait-on signal to a {@link PCollection}.
   *
   * @param input the collection to apply the wait on.
   * @param transformId a unique identifier for the wait transform.
   * @return the collection with the wait applied, if configured.
   */
  private <V extends PCollection> V wait(V input, String transformId) {
    if (waitOn() == null) {
      return input;
    } else {
      return (V) input.apply(getTransformName("Wait." + transformId, null, null), waitOn());
    }
  }

  private PCollection<?> peekRanges(PCollection<KV<Integer, ImmutableList<Range>>> ranges) {
    if (additionalOperationsOnRanges() != null) {
      return (PCollection<?>)
          ranges.apply(
              getTransformName("AdditionalOps", null, null), additionalOperationsOnRanges());
    }
    // Return a dummy, empty PCollection that can be waited on without effect.
    return ranges
        .getPipeline()
        .apply(getTransformName("CreateVoid", null, null), Create.of((Void) null));
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> setDataSourceProviderFn(
        SerializableFunction<Void, DataSource> value);

    public abstract Builder<T> setDbAdapter(UniformSplitterDBAdapter value);

    public abstract Builder<T> setCountQueryTimeoutMillis(long value);

    public abstract Builder<T> setTableSplitSpecifications(
        ImmutableList<TableSplitSpecification> value);

    abstract ImmutableList<TableSplitSpecification> tableSplitSpecifications();

    public abstract Builder<T> setAutoAdjustMaxPartitions(Boolean value);

    public abstract Builder<T> setDbParallelizationForSplitProcess(@Nullable Integer value);

    public abstract Builder<T> setDbParallelizationForReads(@Nullable Integer value);

    public abstract Builder<T> setTableReadSpecifications(
        ImmutableMap<TableIdentifier, TableReadSpecification<T>> value);

    abstract ImmutableMap<TableIdentifier, TableReadSpecification<T>> tableReadSpecifications();

    public abstract Builder<T> setAdditionalOperationsOnRanges(
        @Nullable PTransform<PCollection<KV<Integer, ImmutableList<Range>>>, ?> value);

    public abstract Builder<T> setWaitOn(@Nullable OnSignal<?> value);

    public abstract Builder<T> setNumBatches(long value);

    public abstract Builder<T> setTransformPrefix(@Nullable String value);

    @Nullable
    abstract String transformPrefix();

    abstract ReadWithUniformPartitions<T> autoBuild();

    @VisibleForTesting
    protected String getTransformPrefix() {
      if (transformPrefix() != null) {
        return transformPrefix();
      }
      return "RWUP."
          + Objects.hash(
              tableSplitSpecifications().stream()
                  .map(s -> s.tableIdentifier().tableName())
                  .sorted()
                  .collect(Collectors.toList()));
    }

    public ReadWithUniformPartitions<T> build() {
      Preconditions.checkState(
          !tableSplitSpecifications().isEmpty(), "tableSplitSpecifications cannot be empty");
      Preconditions.checkState(
          !tableReadSpecifications().isEmpty(), "tableReadSpecifications cannot be empty");
      Preconditions.checkState(
          tableSplitSpecifications().size() == tableReadSpecifications().size(),
          "Size of tableSplitSpecifications and tableReadSpecifications must match");

      java.util.Set<TableIdentifier> splitIdentifiers =
          tableSplitSpecifications().stream()
              .map(TableSplitSpecification::tableIdentifier)
              .collect(Collectors.toSet());
      Preconditions.checkState(
          splitIdentifiers.equals(tableReadSpecifications().keySet()),
          "TableIdentifiers in tableSplitSpecifications and tableReadSpecifications must match");

      setTransformPrefix(getTransformPrefix());

      ReadWithUniformPartitions readWithUniformPartitions = autoBuild();
      logger.info("Initialized ReadWithUniformPartitions {}", readWithUniformPartitions);
      return readWithUniformPartitions;
    }
  }
}
