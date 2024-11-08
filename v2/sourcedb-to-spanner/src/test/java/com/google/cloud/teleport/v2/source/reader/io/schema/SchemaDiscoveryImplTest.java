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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryRetriesExhaustedException;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SchemaDiscoveryImpl}. */
@RunWith(MockitoJUnitRunner.class)
public class SchemaDiscoveryImplTest {
  @Mock RetriableSchemaDiscovery mockRetriableSchemaDiscovery;

  @Mock DataSource mockDataSource;
  @Mock javax.sql.DataSource mockJdbcDataSource;

  @Mock SourceSchemaReference mockSourceSchemaReference;

  @Test
  public void testSchemaDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    when(mockDataSource.jdbc()).thenReturn(mockJdbcDataSource);
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(ImmutableMap.of());
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableSchema(mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .isEqualTo(ImmutableMap.of());
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testSchemaDiscoveryImplThrowsRetriesExhausted()
      throws SQLException, IOException, RetriableSchemaDiscoveryException {

    final FluentBackoff mockFluentBackoff = mock(FluentBackoff.class);
    final BackOff mockBackoff = mock(BackOff.class);

    when(mockFluentBackoff.backoff()).thenReturn(mockBackoff);
    when(mockBackoff.nextBackOffMillis()).thenThrow(new IOException("test"));
    when(mockDataSource.jdbc()).thenReturn(mockJdbcDataSource);
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, mockFluentBackoff)
                .discoverTableSchema(
                    mockDataSource, mockSourceSchemaReference, ImmutableList.of()));
    verify(mockRetriableSchemaDiscovery, times(1 /* No Retries */))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testSchemaDiscoveryImplHandlesIOException()
      throws SQLException, IOException, RetriableSchemaDiscoveryException {

    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    when(mockDataSource.jdbc()).thenReturn(mockJdbcDataSource);
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableSchema(
                    mockDataSource, mockSourceSchemaReference, ImmutableList.of()));
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testTableDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableList<String> testTables = ImmutableList.of("testTable1", "testTable2");
    when(mockDataSource.jdbc()).thenReturn(mockJdbcDataSource);
    when(mockRetriableSchemaDiscovery.discoverTables(mockDataSource, mockSourceSchemaReference))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(testTables);
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTables(mockDataSource, mockSourceSchemaReference))
        .isEqualTo(testTables);
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount)).discoverTables(any(), any());
  }

  @Test
  public void testIndexDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoveredIndexes =
        ImmutableMap.of(
            "testTable1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("testCol")
                    .setIndexName("PRIMARY")
                    .setIsPrimary(true)
                    .setIsUnique(true)
                    .setCardinality(42L)
                    .setOrdinalPosition(1)
                    .setIndexType(IndexType.NUMERIC)
                    .build()));
    when(mockDataSource.jdbc()).thenReturn(mockJdbcDataSource);
    when(mockRetriableSchemaDiscovery.discoverTableIndexes(
            mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable1")))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(discoveredIndexes);
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableIndexes(
                    mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable1")))
        .isEqualTo(discoveredIndexes);
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableIndexes(any(), any(), any());
  }
}
