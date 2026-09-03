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
package com.google.cloud.teleport.spanner.ddl;

import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

/** Unit tests for {@link LocalityGroup} class. */
public class LocalityGroupTest {

  @Test
  public void testBasicLocalityGroup() {
    LocalityGroup lg = LocalityGroup.builder().name("lg1").build();

    assertThat(lg.prettyPrint(), equalToCompressingWhiteSpace("CREATE LOCALITY GROUP `lg1`"));
    assertThat(
        lg.toBuilder().build().toString(),
        equalToCompressingWhiteSpace("CREATE LOCALITY GROUP `lg1`"));
  }

  @Test
  public void testBasicLocalityGroupPg() {
    LocalityGroup lg = LocalityGroup.builder(Dialect.POSTGRESQL).name("lg1").build();

    assertThat(lg.prettyPrint(), equalToCompressingWhiteSpace("CREATE LOCALITY GROUP \"lg1\""));
    assertEquals(Dialect.POSTGRESQL, lg.dialect());
  }

  @Test
  public void testLocalityGroupWithOptions() {
    LocalityGroup lg =
        LocalityGroup.builder()
            .name("lg1")
            .options(
                ImmutableList.of(
                    "storage = 'ssd'", "ssd_to_hdd_spill_timespan = '30d'"))
            .build();

    assertThat(
        lg.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE LOCALITY GROUP `lg1`\n\tOPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '30d')"));
  }

  @Test
  public void testLocalityGroupWithOptionsPg() {
    LocalityGroup lg =
        LocalityGroup.builder(Dialect.POSTGRESQL)
            .name("lg1")
            .options(ImmutableList.of("storage = 'ssd'"))
            .build();

    assertThat(
        lg.prettyPrint(),
        equalToCompressingWhiteSpace("CREATE LOCALITY GROUP \"lg1\"\n\tOPTIONS (storage = 'ssd')"));
  }

  @Test
  public void testEqualsAndHashCode() {
    LocalityGroup lg1 =
        LocalityGroup.builder()
            .name("lg1")
            .options(ImmutableList.of("storage = 'ssd'"))
            .build();
    LocalityGroup lg2 =
        LocalityGroup.builder()
            .name("lg1")
            .options(ImmutableList.of("storage = 'ssd'"))
            .build();
    LocalityGroup lg3 =
        LocalityGroup.builder()
            .name("lg2")
            .options(ImmutableList.of("storage = 'ssd'"))
            .build();

    assertEquals(lg1, lg2);
    assertEquals(lg1.hashCode(), lg2.hashCode());
    assertNotEquals(lg1, lg3);
  }
}
