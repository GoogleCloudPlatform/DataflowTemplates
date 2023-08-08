/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.job;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TargetTest {

  @Parameters(name = "targetOfType({0}).compareTo(targetOfType({1})) == {2}")
  public static Collection<Object[]> parameters() {
    TargetType customQuery = TargetType.custom;
    TargetType node = TargetType.node;
    TargetType edge = TargetType.edge;
    return List.of(
        new Object[][] {
          // customQuery comes-after edge comes-after node
          {customQuery, customQuery, 0},
          {customQuery, node, 1},
          {customQuery, edge, 1},
          {node, node, 0},
          {node, edge, -1},
          {node, customQuery, -1},
          {edge, edge, 0},
          {edge, node, 1},
          {edge, customQuery, -1}
        });
  }

  private final Target left;
  private final Target right;
  private final int comparison;

  public TargetTest(TargetType left, TargetType right, int comparison) {
    this.left = targetOfType(left);
    this.right = targetOfType(right);
    this.comparison = comparison;
  }

  @Test
  public void ordersTargets() {
    assertThat(left.compareTo(right)).isEqualTo(comparison);
  }

  private static Target targetOfType(TargetType type) {
    Target target = new Target();
    target.setType(type);
    return target;
  }
}
