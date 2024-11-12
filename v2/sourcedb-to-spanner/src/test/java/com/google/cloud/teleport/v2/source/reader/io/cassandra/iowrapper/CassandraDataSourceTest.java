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

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

/** Test class for {@link CassandraDataSource}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraDataSourceTest {
  @Test
  public void testCassandraDataSoureBasic() {
    String testCluster = "testCluster";
    String testHost = "testHost";
    int testPort = 9042;
    CassandraDataSource cassandraDataSource =
        CassandraDataSource.builder()
            .setClusterName(testCluster)
            .setContactPoints(List.of(new InetSocketAddress(testHost, testPort)))
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("test-user-name")
                    .setPassword("test")
                    .build())
            .build();
    assertThat(cassandraDataSource.clusterName()).isEqualTo(testCluster);
    assertThat(cassandraDataSource.contactPoints())
        .isEqualTo(ImmutableList.of(new InetSocketAddress(testHost, testPort)));
    assertThat(cassandraDataSource.dbAuth().getUserName().get()).isEqualTo("test-user-name");
    assertThat(cassandraDataSource.dbAuth().getPassword().get()).isEqualTo("test");
  }
}
