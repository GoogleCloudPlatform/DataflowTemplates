/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link TableIdentifier}. */
@RunWith(MockitoJUnitRunner.class)
public class TableIdentifierTest {

  @Test
  public void testTableIdentifierEquality() {
    TableIdentifier identifier1 =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("table1")
            .build();
    TableIdentifier identifier2 =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("table1")
            .build();
    TableIdentifier identifier3 =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("table2")
            .build();

    // Test for self-equality and equality with another identical object
    assertThat(identifier1).isEqualTo(identifier1);
    assertThat(identifier1).isEqualTo(identifier2);
    assertThat(identifier1.hashCode()).isEqualTo(identifier2.hashCode());

    // Test for inequality with a different object
    assertThat(identifier1).isNotEqualTo(identifier3);
    assertThat(identifier1.hashCode()).isNotEqualTo(identifier3.hashCode());

    // Test for inequality with null
    assertThat(identifier1).isNotEqualTo(null);
  }

  @Test
  public void testTableIdentifierComparison() {
    TableIdentifier identifierA =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("tableA")
            .build();
    TableIdentifier identifierB =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("tableB")
            .build();
    TableIdentifier identifierC =
        TableIdentifier.builder()
            .setDataSourceId("a1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("tableB")
            .build();
    TableIdentifier anotherIdentifierA =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("tableA")
            .build();

    assertThat(identifierA).isEquivalentAccordingToCompareTo(anotherIdentifierA);
    assertThat(identifierA).isLessThan(identifierB);
    assertThat(identifierB).isGreaterThan(identifierA);
    assertThat(identifierC).isLessThan(identifierA);
    assertThat(identifierC).isLessThan(identifierB);
  }
}
