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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataSourceProviderImpl}. */
@RunWith(JUnit4.class)
public class DataSourceProviderImplTest {

  /**
   * Tests that the provider correctly returns the expected {@link DataSource} instances for valid
   * identifiers.
   */
  @Test
  public void testGetDataSource_success() {
    DataSource mockDs1 = mock(DataSource.class);
    DataSource mockDs2 = mock(DataSource.class);

    SerializableFunction<Void, DataSource> provider1 = _unused -> mockDs1;
    SerializableFunction<Void, DataSource> provider2 = _unused -> mockDs2;

    DataSourceProviderImpl provider =
        DataSourceProviderImpl.builder()
            .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", provider1)
            .addDataSource("4560d550-7e9d-4129-b5da-3f6468f3a8cf", provider2)
            .build();

    assertThat(provider.getDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458"))
        .isSameInstanceAs(mockDs1);
    assertThat(provider.getDataSource("4560d550-7e9d-4129-b5da-3f6468f3a8cf"))
        .isSameInstanceAs(mockDs2);
    assertThat(provider.getDataSourceIds())
        .isEqualTo(
            ImmutableSet.of(
                "b1a1ec3b-195d-4755-b04b-02bc64dc4458", "4560d550-7e9d-4129-b5da-3f6468f3a8cf"));
  }

  /**
   * Tests that attempting to retrieve a {@link DataSource} for an unknown identifier throws an
   * {@link IllegalArgumentException}.
   */
  @Test
  public void testGetDataSource_unknownId_throwsException() {
    DataSourceProviderImpl provider = DataSourceProviderImpl.builder().build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> provider.getDataSource("unknown"));
    assertThat(thrown).hasMessageThat().contains("Unknown datasourceId: unknown");
  }

  /**
   * Tests that the {@link DataSourceProviderImpl} can be correctly serialized and deserialized
   * while maintaining its functionality.
   */
  @Test
  public void testSerialization() throws Exception {
    // Need a truly serializable function for this test
    SerializableFunction<Void, DataSource> serializableProvider = new TestDataSourceProvider();

    DataSourceProviderImpl provider =
        DataSourceProviderImpl.builder()
            .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", serializableProvider)
            .build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(provider);
    }

    DataSourceProviderImpl deserialized;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      deserialized = (DataSourceProviderImpl) ois.readObject();
    }

    assertThat(deserialized.getDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458"))
        .isSameInstanceAs(provider.getDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458"));
  }

  /** Tests that the builder prevents adding multiple providers for the same shard identifier. */
  @Test
  public void testDuplicateDataSourceId_throwsException() {
    DataSourceProviderImpl.Builder builder = DataSourceProviderImpl.builder();
    SerializableFunction<Void, DataSource> provider = _unused -> null;

    builder.addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", provider);
    builder.addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", provider);
    assertThrows(IllegalArgumentException.class, () -> builder.build());
  }

  /** A simple serializable provider for testing. */
  private static class TestDataSourceProvider
      implements SerializableFunction<Void, DataSource>, Serializable {
    @Override
    public DataSource apply(Void input) {
      return null;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestDataSourceProvider;
    }

    @Override
    public int hashCode() {
      return TestDataSourceProvider.class.hashCode();
    }
  }

  /** Tests that display data is correctly populated from the configured providers. */
  @Test
  public void testPopulateDisplayData() {
    TestDataSourceProviderWithDisplayData providerWithDisplayData =
        new TestDataSourceProviderWithDisplayData();
    DataSourceProviderImpl provider =
        DataSourceProviderImpl.builder()
            .addDataSource("shard1", providerWithDisplayData)
            .addDataSource("shard2", new TestDataSourceProvider())
            .build();

    DisplayData displayData = DisplayData.from(provider);
    java.util.Collection<DisplayData.Item> items = displayData.items();

    assertThat(
            items.stream()
                .anyMatch(
                    i ->
                        "testKey".equals(i.getKey())
                            && "testValue".equals(i.getValue())
                            && i.getPath().toString().contains("dataSource-shard1")))
        .isTrue();
  }

  /** A simple serializable provider with display data for testing. */
  private static class TestDataSourceProviderWithDisplayData
      implements SerializableFunction<Void, DataSource>, Serializable, HasDisplayData {
    @Override
    public DataSource apply(Void input) {
      return null;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("testKey", "testValue"));
    }
  }
}
