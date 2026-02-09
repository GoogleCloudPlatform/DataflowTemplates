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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryRetriesExhaustedException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Implementation for {@link SchemaDiscovery}. {@link SchemaDiscoveryImpl} wraps an
 * implementation of {@link RetriableSchemaDiscovery} and implements the retries as configured by
 * {@link FluentBackoff}.
 *
 * <p><b>Note:</b>An implementatin of {@link RetriableSchemaDiscovery} must return {@link
 * RetriableSchemaDiscoveryException} for retriable errors.
 */
public final class SchemaDiscoveryImpl implements SchemaDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaDiscoveryImpl.class);
  private final RetriableSchemaDiscovery retriableSchemaDiscovery;
  private final FluentBackoff fluentBackoff;
  private static final int BATCH_SIZE = 500;
  private static final int THREAD_POOL_SIZE = 4;

  public SchemaDiscoveryImpl(
      RetriableSchemaDiscovery retriableSchemaDiscovery, FluentBackoff fluentBackoff) {
    this.retriableSchemaDiscovery = retriableSchemaDiscovery;
    this.fluentBackoff = fluentBackoff;
  }

  /**
   * Discover Tables to migrate. This method could be used to auto infer tables to migrate if not
   * passed via options.
   *
   * @param dataSource Provider for source connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @return The list of table names for the given database.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>The Implementations must log every exception and generate metrics as appropriate. Any
   *     retriable error must be retried as needed.
   */
  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException {
    return doRetries(
        () -> retriableSchemaDiscovery.discoverTables(dataSource, sourceSchemaReference));
  }

  /**
   * Discover the schema of tables to migrate.
   *
   * <p>To support large-scale migrations (e.g., 5,000+ tables), this method performs "Bulk
   * Discovery." It partitions the list of tables into batches and executes schema discovery for
   * each batch in parallel using a managed thread pool. This significantly reduces the total
   * initialization time by avoiding the overhead of thousands of individual sequential database
   * queries.
   *
   * @param dataSource - Provider for source connection.
   * @param sourceSchemaReference - Source database name and (optionally namespace)
   * @param tables - Tables to migrate.
   * @return - The discovered schema
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   */
  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryRetriesExhaustedException {
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    // Partition the tables into batches to enable parallel discovery via SQL IN clauses.
    List<List<String>> batches = partitionWork(tables, BATCH_SIZE);

    // We use a fixed thread pool to bound the resource impact on the Dataflow Launcher VM,
    // which typically has limited vCPU and memory.
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    try {
      List<CompletableFuture<ImmutableMap<String, ImmutableMap<String, SourceColumnType>>>>
          futures = new ArrayList<>();
      for (List<String> batch : batches) {
        ImmutableList<String> immutableBatch = ImmutableList.copyOf(batch);
        futures.add(
            CompletableFuture.supplyAsync(
                () -> {
                  return doRetries(
                      () ->
                          retriableSchemaDiscovery.discoverTableSchema(
                              dataSource, sourceSchemaReference, immutableBatch));
                },
                executor));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      for (CompletableFuture<ImmutableMap<String, ImmutableMap<String, SourceColumnType>>> future :
          futures) {
        result.putAll(future.join());
      }
    } catch (Exception e) {
      convertException(e);
    } finally {
      // Ensure the executor is always shut down to prevent thread leaks in the launcher VM.
      executor.shutdown();
    }
    return result.build();
  }

  @VisibleForTesting
  protected static void convertException(Exception e) {
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof SchemaDiscoveryException) {
        throw (SchemaDiscoveryException) cause;
      }
      if (cause instanceof SchemaDiscoveryRetriesExhaustedException) {
        throw (SchemaDiscoveryRetriesExhaustedException) cause;
      }
      if (cause instanceof java.util.concurrent.CompletionException
          || cause instanceof java.util.concurrent.ExecutionException
          || cause instanceof RuntimeException) {
        cause = cause.getCause();
      } else {
        break;
      }
    }
    throw new SchemaDiscoveryException(e);
  }

  /**
   * Discover the indexes of tables to migrate.
   *
   * <p>Similar to {@link #discoverTableSchema}, this method uses parallel batch processing to
   * efficiently discover indexes for a large number of tables.
   *
   * @param dataSource Provider for source connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered indexes.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   */
  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryRetriesExhaustedException {
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();
    List<List<String>> batches = partitionWork(tables, BATCH_SIZE);
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    try {
      List<CompletableFuture<ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>>>> futures =
          new ArrayList<>();
      for (List<String> batch : batches) {
        ImmutableList<String> immutableBatch = ImmutableList.copyOf(batch);
        futures.add(
            CompletableFuture.supplyAsync(
                () -> {
                  return doRetries(
                      () ->
                          retriableSchemaDiscovery.discoverTableIndexes(
                              dataSource, sourceSchemaReference, immutableBatch));
                },
                executor));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      for (CompletableFuture<ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>>> future :
          futures) {
        result.putAll(future.join());
      }
    } catch (Exception e) {
      convertException(e);
    } finally {
      executor.shutdown();
    }
    return result.build();
  }

  /**
   * Partitions a list of items into smaller batches of a specified size. This is used to break down
   * large table discovery tasks into manageable chunks for parallel execution.
   *
   * @param <T> the type of items in the list.
   * @param items the list of items to partition.
   * @param batchSize the maximum size of each batch.
   * @return a list of batches.
   */
  @VisibleForTesting
  protected static <T> List<List<T>> partitionWork(List<T> items, int batchSize) {
    return Lists.partition(items, batchSize);
  }

  private <T> T doRetries(SchemaDiscoveryOperation<T> operation) throws SchemaDiscoveryException {

    BackOff backoff = this.fluentBackoff.backoff();
    do {
      try {
        return operation.call();
      } catch (RetriableSchemaDiscoveryException e) {
        try {
          long nextBackOffMillis = backoff.nextBackOffMillis();
          if (nextBackOffMillis != BackOff.STOP) {
            Thread.sleep(nextBackOffMillis);
          } else {
            throw new SchemaDiscoveryRetriesExhaustedException(e);
          }
        } catch (InterruptedException threadException) {
          /* If sleep is interrupted, get back to work.
           * Unit testing this catch-point will need intrusive setting of thread state.
           */
        } catch (Exception exception) {
          throw new SchemaDiscoveryRetriesExhaustedException(exception);
        }
      }
    } while (true);
  }

  /** Similar to Callable but with narrowed set of exceptions. */
  interface SchemaDiscoveryOperation<T> {
    T call() throws RetriableSchemaDiscoveryException, SchemaDiscoveryException;
  }
}
