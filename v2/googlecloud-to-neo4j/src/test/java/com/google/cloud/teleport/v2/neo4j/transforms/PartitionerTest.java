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
package com.google.cloud.teleport.v2.neo4j.transforms;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import org.junit.Test;

public class PartitionerTest {

  @Test
  public void partitions_empty_iterable_yields_empty_partitions() {
    List<Object> elements = List.of();

    Iterable<Iterable<Object>> partitions = Partitioner.partition(elements, 1);

    assertThat(partitions).isEmpty();
  }

  @Test
  public void partitions_single_partition_when_size_equals_input_size() {
    List<Object> elements = List.of("1", "2", "3", "4");

    Iterable<Iterable<Object>> partitions = Partitioner.partition(elements, 4);

    assertThat(partitions).isEqualTo(List.of(List.of("1", "2", "3", "4")));
  }

  @Test
  public void partitions_single_partition_when_size_is_larger_than_input_size() {
    List<Object> elements = List.of("1", "2", "3", "4");

    Iterable<Iterable<Object>> partitions = Partitioner.partition(elements, 42);

    assertThat(partitions).isEqualTo(List.of(List.of("1", "2", "3", "4")));
  }

  @Test
  public void partitions_into_same_sized_partitions() {
    List<Object> elements = List.of("1", "2", "3", "4");

    Iterable<Iterable<Object>> partitions = Partitioner.partition(elements, 2);

    assertThat(partitions).isEqualTo(List.of(List.of("1", "2"), List.of("3", "4")));
  }

  @Test
  public void partitions_includes_last_elements_when_size_is_not_multiple_of_input_size() {
    List<Object> elements = List.of("1", "2", "3", "4", "5");

    Iterable<Iterable<Object>> partitions = Partitioner.partition(elements, 2);

    assertThat(partitions).isEqualTo(List.of(List.of("1", "2"), List.of("3", "4"), List.of("5")));
  }
}
