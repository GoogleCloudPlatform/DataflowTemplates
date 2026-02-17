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
import static org.mockito.Mockito.mockStatic;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.common.io.Resources;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.junit.Ignore;

/** Test class for {@link CassandraDataSourceOss}. */
@RunWith(MockitoJUnitRunner.class)
@Ignore("Temporarily disabled for maintenance")
public class CassandraDataSourceOssTest {

  @Test
  public void testCassandraDataSourceOssBasic() {
    String testCluster = "testCluster";
    String testHost = "127.0.0.1";
    int testPort = 9042;
    CassandraDataSourceOss cassandraDataSourceOss =
        CassandraDataSourceOss.builder()
            .setClusterName(testCluster)
            .setOptionsMap(OptionsMap.driverDefaults())
            .setContactPoints(List.of(new InetSocketAddress(testHost, testPort)))
            .overrideOptionInOptionsMap(TypedDriverOption.AUTH_PROVIDER_USER_NAME, "test-user-name")
            .overrideOptionInOptionsMap(TypedDriverOption.AUTH_PROVIDER_PASSWORD, "test")
            .build();
    assertThat(cassandraDataSourceOss.clusterName()).isEqualTo(testCluster);
    assertThat(cassandraDataSourceOss.contactPoints())
        .isEqualTo(ImmutableList.of(new InetSocketAddress(testHost, testPort)));
    assertThat(
            cassandraDataSourceOss
                .driverConfigLoader()
                .getInitialConfig()
                .getDefaultProfile()
                .getString(TypedDriverOption.AUTH_PROVIDER_USER_NAME.getRawOption()))
        .isEqualTo("test-user-name");
    assertThat(
            cassandraDataSourceOss
                .driverConfigLoader()
                .getInitialConfig()
                .getDefaultProfile()
                .getString(TypedDriverOption.AUTH_PROVIDER_PASSWORD.getRawOption()))
        .isEqualTo("test");
    assertThat(cassandraDataSourceOss.numPartitions()).isNull();
    assertThat(cassandraDataSourceOss.toBuilder().setNumPartitions(42).build().numPartitions())
        .isEqualTo(42);
  }

  @Test
  public void testLoadFromGCS() throws FileNotFoundException {
    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("CassandraUT/test-cassandra-config.conf");
    try (MockedStatic mockFileReader = mockStatic(JarFileReader.class)) {
      mockFileReader
          .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
          .thenReturn(new URL[] {testUrl});
      CassandraDataSourceOss cassandraDataSourceOss =
          CassandraDataSourceOss.builder().setOptionsMapFromGcsFile(testGcsPath).build();

      assertThat(cassandraDataSourceOss.loggedKeySpace()).isEqualTo("test-keyspace");
      assertThat(cassandraDataSourceOss.localDataCenter()).isEqualTo("datacenter1");
      assertThat(cassandraDataSourceOss.contactPoints())
          .isEqualTo(
              ImmutableList.of(
                  new InetSocketAddress("127.0.0.1", 9042),
                  new InetSocketAddress("127.0.0.1", 9043)));
    }
  }
}
