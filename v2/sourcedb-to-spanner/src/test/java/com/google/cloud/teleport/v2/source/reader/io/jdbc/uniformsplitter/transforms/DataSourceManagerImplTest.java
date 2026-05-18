/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataSourceManagerImpl}. */
@RunWith(JUnit4.class)
public class DataSourceManagerImplTest {

  private DataSourceProvider mockProvider;
  private DataSource mockDataSource;

  @Before
  public void setUp() {
    mockProvider = mock(DataSourceProvider.class);
    mockDataSource = mock(DataSource.class);
  }

  /**
   * Tests that {@link DataSourceManagerImpl#getDatasource} correctly initializes a new data source
   * on the first call and returns the cached instance on subsequent calls.
   */
  @Test
  public void testGetDatasource_initializesAndCaches() {
    DataSourceManagerImpl manager =
        DataSourceManagerImpl.builder().setDataSourceProvider(mockProvider).build();
    when(mockProvider.getDataSource("ds1")).thenReturn(mockDataSource);

    // First call should call provider
    DataSource ds1 = manager.getDatasource("ds1");
    assertThat(ds1).isSameInstanceAs(mockDataSource);
    verify(mockProvider, times(1)).getDataSource("ds1");

    // Second call should return cached instance
    DataSource ds2 = manager.getDatasource("ds1");
    assertThat(ds2).isSameInstanceAs(mockDataSource);
    verify(mockProvider, times(1)).getDataSource("ds1");
  }

  /**
   * Tests that {@link DataSourceManagerImpl#closeAll} correctly identifies and closes all {@link
   * Closeable} data sources managed by the instance.
   *
   * <p>Note: We use a custom interface {@link CloseableDataSource} and verify the {@code close()}
   * call explicitly. While Mockito could be used more extensively, this manual verification with
   * mocks ensures that the implementation correctly handles the {@link Closeable} contract.
   */
  @Test
  public void testCloseAll_closesCloseableDataSources() throws IOException {
    DataSourceManagerImpl manager =
        DataSourceManagerImpl.builder().setDataSourceProvider(mockProvider).build();
    CloseableDataSource mockCloseableDs = mock(CloseableDataSource.class);
    DataSource mockNonCloseableDs = mock(DataSource.class);

    when(mockProvider.getDataSource("ds1")).thenReturn(mockCloseableDs);
    when(mockProvider.getDataSource("ds2")).thenReturn(mockNonCloseableDs);

    manager.getDatasource("ds1");
    manager.getDatasource("ds2");

    manager.closeAll();

    verify(mockCloseableDs, times(1)).close();
    // Non-closeable should not cause issues (nothing to verify really, just that it doesn't crash)
  }

  /**
   * Tests that {@link DataSourceManagerImpl#closeAll} gracefully handles {@link IOException}s
   * thrown during the closure of a data source.
   */
  @Test
  public void testCloseAll_handlesIOException() throws IOException {
    DataSourceManagerImpl manager =
        DataSourceManagerImpl.builder().setDataSourceProvider(mockProvider).build();
    CloseableDataSource mockCloseableDs = mock(CloseableDataSource.class);

    when(mockProvider.getDataSource("ds1")).thenReturn(mockCloseableDs);
    doThrow(new IOException("Test exception")).when(mockCloseableDs).close();

    manager.getDatasource("ds1");

    // Should not throw exception
    manager.closeAll();

    verify(mockCloseableDs, times(1)).close();
  }

  /**
   * Tests the behavior of {@link DataSourceManagerImpl} after serialization and deserialization,
   * specifically verifying that transient fields are correctly handled.
   */
  @Test
  public void testSerialization_transientFieldsBecomeNull() throws Exception {
    // We need a serializable provider for this test
    SerializableDataSourceProvider serializableProvider = new SerializableDataSourceProvider();
    DataSourceManagerImpl manager =
        DataSourceManagerImpl.builder().setDataSourceProvider(serializableProvider).build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(manager);
    }

    DataSourceManagerImpl deserialized;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      deserialized = (DataSourceManagerImpl) ois.readObject();
    }

    // closeAll should handle null transient fields gracefully
    deserialized.closeAll();

    // getDatasource is expected to fail with NPE if transient fields are null and not
    // re-initialized
    assertThrows(NullPointerException.class, () -> deserialized.getDatasource("any"));
  }

  /**
   * Tests that {@link DataSourceManagerImpl#getDatasource} is thread-safe and ensures only one data
   * source is created even when accessed concurrently by multiple threads.
   */
  @Test
  public void testGetDatasource_concurrency() throws InterruptedException {
    DataSourceManagerImpl manager =
        DataSourceManagerImpl.builder().setDataSourceProvider(mockProvider).build();
    when(mockProvider.getDataSource(anyString())).thenReturn(mockDataSource);

    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(() -> manager.getDatasource("ds1"));
      threads[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }

    // Provider should be called only once even with multiple threads
    verify(mockProvider, times(1)).getDataSource("ds1");
  }

  /** Interface for mocking a Closeable DataSource. */
  private interface CloseableDataSource extends DataSource, Closeable {}

  /** A serializable provider for testing serialization. */
  private static class SerializableDataSourceProvider implements DataSourceProvider, Serializable {
    @Override
    public DataSource getDataSource(String dataSourceId) {
      return null;
    }

    @Override
    public com.google.common.collect.ImmutableSet<String> getDataSourceIds() {
      return com.google.common.collect.ImmutableSet.of();
    }
  }
}
