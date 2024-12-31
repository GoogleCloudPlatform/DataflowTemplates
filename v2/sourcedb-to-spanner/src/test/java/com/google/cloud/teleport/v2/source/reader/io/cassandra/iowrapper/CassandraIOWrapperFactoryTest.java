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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraIOWrapperFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraIOWrapperFactoryTest {
  @Test
  public void testCassandraIoWrapperFactoryBasic() {
    String testConfigPath = "gs://smt-test-bucket/test-conf.conf";
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSourceDbDialect()).thenReturn("CASSANDRA");
    when(mockOptions.getSourceConfigURL()).thenReturn(testConfigPath);
    CassandraIOWrapperFactory cassandraIOWrapperFactory =
        CassandraIOWrapperFactory.fromPipelineOptions(mockOptions);
    assertThat(cassandraIOWrapperFactory.gcsConfigPath()).isEqualTo(testConfigPath);
    assertThat(cassandraIOWrapperFactory.getIOWrapper(List.of(), null)).isEqualTo(null);
  }

  @Test
  public void testCassandraIoWrapperFactoryExceptions() {
    String testConfigPath = "smt-test-bucket/test-conf.conf";
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSourceDbDialect()).thenReturn("MYSQL").thenReturn("CASSANDRA");
    when(mockOptions.getSourceConfigURL()).thenReturn(testConfigPath);
    assertThrows(
        IllegalArgumentException.class,
        () -> CassandraIOWrapperFactory.fromPipelineOptions(mockOptions));
    assertThrows(
        IllegalArgumentException.class,
        () -> CassandraIOWrapperFactory.fromPipelineOptions(mockOptions));
  }
}
