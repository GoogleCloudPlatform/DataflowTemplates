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
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.dtsx.astra.sdk.db.DbOpsClient;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.exception.AstraDBNotFoundException;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link AstraDbDataSource}. */
@RunWith(MockitoJUnitRunner.class)
public class AstraDbDataSourceTest {

  @Test
  public void testAstraDbDataSourceBasic() {
    String testToken = "AstraCS:testToken";
    String testDatabaseID = "testID";
    String testKeySpace = "testKeySpace";
    String testRegion = "us-east1";
    Integer testNumPartitions = 42;
    byte[] testSecureBundleWithRegion = "Secure-Bundle-With-Region".getBytes();
    byte[] testSecureBundleWithoutRegion = "Secure-Bundle-Default".getBytes();

    try (MockedConstruction<DbOpsClient> mockedConstruction =
        mockConstruction(
            DbOpsClient.class,
            (mock, context) -> {
              when(mock.exist()).thenReturn(true);
              when(mock.downloadSecureConnectBundle(testRegion))
                  .thenReturn(testSecureBundleWithRegion);
              when(mock.downloadDefaultSecureConnectBundle())
                  .thenReturn(testSecureBundleWithoutRegion);
            })) {
      AstraDbDataSource astraDbDataSourceWithoutRegion =
          AstraDbDataSource.builder()
              .setAstraToken(testToken)
              .setDatabaseId(testDatabaseID)
              .setNumPartitions(testNumPartitions)
              .setKeySpace(testKeySpace)
              .build();
      AstraDbDataSource astraDbDataSourceWithRegion =
          astraDbDataSourceWithoutRegion.toBuilder().setAstraDbRegion(testRegion).autoBuild();

      assertThat(astraDbDataSourceWithRegion.secureConnectBundle())
          .isEqualTo(testSecureBundleWithRegion);
      assertThat(astraDbDataSourceWithoutRegion.secureConnectBundle())
          .isEqualTo(testSecureBundleWithoutRegion);
      assertThat(astraDbDataSourceWithRegion.astraDbRegion()).isEqualTo(testRegion);
      assertThat(astraDbDataSourceWithRegion.astraToken().get()).isEqualTo(testToken);
      assertThat(astraDbDataSourceWithRegion.databaseId()).isEqualTo(testDatabaseID);
      assertThat(astraDbDataSourceWithRegion.numPartitions()).isEqualTo(testNumPartitions);
      assertThat(astraDbDataSourceWithRegion.keySpace()).isEqualTo(testKeySpace);
    }
  }

  @Test
  public void testAstraDbDataSourceSecretManager() {
    String testToken = "testToken";
    String testDatabaseID = "testID";
    String testKeySpace = "testKeySpace";
    String testRegion = "us-east1";
    Integer testNumPartitions = 42;
    byte[] testSecureBundleWithRegion = "Secure-Bundle-With-Region".getBytes();
    try (MockedStatic<SecretManagerUtils> mockedStatic = mockStatic(SecretManagerUtils.class)) {
      mockedStatic.when(() -> SecretManagerUtils.getSecret(testToken)).thenReturn(testToken);
      try (MockedConstruction<DbOpsClient> mockedConstruction =
          mockConstruction(
              DbOpsClient.class,
              (mock, context) -> {
                when(mock.exist()).thenReturn(true);
                when(mock.downloadSecureConnectBundle(testRegion))
                    .thenReturn(testSecureBundleWithRegion);
              })) {
        AstraDbDataSource astraDbDataSource =
            AstraDbDataSource.builder()
                .setAstraToken(testToken)
                .setDatabaseId(testDatabaseID)
                .setNumPartitions(testNumPartitions)
                .setKeySpace(testKeySpace)
                .setAstraDbRegion(testRegion)
                .build();

        assertThat(astraDbDataSource.secureConnectBundle()).isEqualTo(testSecureBundleWithRegion);
        assertThat(astraDbDataSource.astraDbRegion()).isEqualTo(testRegion);
        assertThat(astraDbDataSource.astraToken().get()).isEqualTo(testToken);
        assertThat(astraDbDataSource.databaseId()).isEqualTo(testDatabaseID);
        assertThat(astraDbDataSource.numPartitions()).isEqualTo(testNumPartitions);
        assertThat(astraDbDataSource.keySpace()).isEqualTo(testKeySpace);
      }
    }
  }

  @Test
  public void testAstraDbDataSourceNonExistant() {
    String testToken = "AstraCS:testToken";
    String testDatabaseID = "testID";
    String testKeySpace = "testKeySpace";
    String testRegion = "us-east1";
    Integer testNumPartitions = 42;
    byte[] testSecureBundleWithRegion = "Secure-Bundle-With-Region".getBytes();
    byte[] testSecureBundleWithoutRegion = "Secure-Bundle-Default".getBytes();

    try (MockedConstruction<DbOpsClient> mockedConstruction =
        mockConstruction(
            DbOpsClient.class,
            (mock, context) -> {
              when(mock.exist()).thenReturn(false);
              when(mock.downloadSecureConnectBundle(testRegion))
                  .thenReturn(testSecureBundleWithRegion);
              when(mock.downloadDefaultSecureConnectBundle())
                  .thenReturn(testSecureBundleWithoutRegion);
            })) {
      AstraDbDataSource astraDbDataSourceNonExistant =
          AstraDbDataSource.builder()
              .setAstraToken(testToken)
              .setDatabaseId(testDatabaseID)
              .setNumPartitions(testNumPartitions)
              .setKeySpace(testKeySpace)
              .setAstraDbRegion("us-west-1")
              .build();
      assertThrows(
          AstraDBNotFoundException.class, () -> astraDbDataSourceNonExistant.secureConnectBundle());
    }
  }
}
