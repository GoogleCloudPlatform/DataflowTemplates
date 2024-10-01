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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link PartitionColumn}. */
@RunWith(MockitoJUnitRunner.class)
public class PartitionColumnTest {
  @Test
  public void testPartitionColumnBasic() {
    PartitionColumn integerPartitionColumn =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Integer.class).build();

    PartitionColumn stringPartitionColumn =
        PartitionColumn.builder()
            .setColumnName("col1")
            .setColumnClass(String.class)
            .setStringCollation(
                CollationReference.builder()
                    .setDbCharacterSet("latin1")
                    .setDbCollation("latin1_swedish_ci")
                    .setPadSpace(true)
                    .build())
            .setStringMaxLength(255)
            .build();

    assertThat(integerPartitionColumn.columnClass()).isEqualTo(Integer.class);
    assertThat(integerPartitionColumn.columnName()).isEqualTo("col1");
    assertThat(integerPartitionColumn.stringCollation()).isNull();
    assertThat(stringPartitionColumn.stringCollation().dbCollation())
        .isEqualTo("latin1_swedish_ci");
    assertThat(stringPartitionColumn.stringMaxLength()).isEqualTo(255);
  }

  @Test
  public void testPartitionColumnPreconditions() {
    assertThrows(
        IllegalStateException.class,
        () -> PartitionColumn.builder().setColumnName("col1").setColumnClass(String.class).build());

    assertThrows(
        IllegalStateException.class,
        () ->
            PartitionColumn.builder()
                .setColumnName("col1")
                .setColumnClass(Integer.class)
                .setStringCollation(
                    CollationReference.builder()
                        .setDbCharacterSet("latin1")
                        .setDbCollation("latin1_general_cs")
                        .setPadSpace(true)
                        .build())
                .build());
    assertThrows(
        IllegalStateException.class,
        () ->
            PartitionColumn.builder()
                .setColumnName("col1")
                .setColumnClass(String.class)
                // NoCollation
                .setStringMaxLength(255)
                .build());
    assertThrows(
        IllegalStateException.class,
        () ->
            PartitionColumn.builder()
                .setColumnName("col1")
                .setColumnClass(String.class)
                .setStringCollation(
                    CollationReference.builder()
                        .setDbCharacterSet("latin1")
                        .setDbCollation("latin1_swedish_ci")
                        .setPadSpace(true)
                        .build())
                // No Max Length.
                .build());
  }
}
