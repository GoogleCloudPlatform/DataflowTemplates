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

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryRetriesExhaustedException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import javax.sql.DataSource;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;

/**
 * Default Implementation for {@link SchemaDiscovery}. {@link SchemaDiscoveryImpl} wraps an
 * implementation of {@link RetriableSchemaDiscovery} and implements the retries as configured by
 * {@link FluentBackoff}.
 *
 * <p><b>Note:</b>An implementatin of {@link RetriableSchemaDiscovery} must return {@link
 * RetriableSchemaDiscoveryException} for retriable errors.
 */
public final class SchemaDiscoveryImpl implements SchemaDiscovery {
  private final RetriableSchemaDiscovery retriableSchemaDiscovery;
  private final FluentBackoff fluentBackoff;

  public SchemaDiscoveryImpl(
      RetriableSchemaDiscovery retriableSchemaDiscovery, FluentBackoff fluentBackoff) {
    this.retriableSchemaDiscovery = retriableSchemaDiscovery;
    this.fluentBackoff = fluentBackoff;
  }

  /**
   * Discover the schema of tables to migrate.
   *
   * @param dataSource - Provider for JDBC connection.
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
      throws SchemaDiscoveryException {

    BackOff backoff = this.fluentBackoff.backoff();
    do {
      try {
        return this.retriableSchemaDiscovery.discoverTableSchema(
            dataSource, sourceSchemaReference, tables);
      } catch (RetriableSchemaDiscoveryException e) {
        try {
          long nextBackOffMillis = backoff.nextBackOffMillis();
          if (nextBackOffMillis != BackOff.STOP) {
            Thread.sleep(nextBackOffMillis);
          } else {
            throw new SchemaDiscoveryRetriesExhaustedException(e);
          }
        } catch (IOException ioException) {
          throw new SchemaDiscoveryRetriesExhaustedException(ioException);
        } catch (InterruptedException threadException) {
          /* If sleep is interrupted, get back to work.
           * Unit testing this catch-point will need intrusive setting of thread state.
           */
        }
      }
    } while (true);
  }
}
