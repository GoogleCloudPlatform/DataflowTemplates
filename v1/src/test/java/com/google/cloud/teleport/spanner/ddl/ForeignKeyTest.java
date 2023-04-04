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
package com.google.cloud.teleport.spanner.ddl;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.spanner.ddl.ForeignKey.ReferentialAction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for ForeignKey class. */
@RunWith(JUnit4.class)
public final class ForeignKeyTest {

  @Test
  public void testParsingOnDeleteCascadeActions() {
    var onDeleteCascadeAction = ReferentialAction.getReferentialAction("DELETE", "CASCADE");
    assertThat(onDeleteCascadeAction)
        .isEquivalentAccordingToCompareTo(ReferentialAction.ON_DELETE_CASCADE);
    var onDeleteNoAction = ReferentialAction.getReferentialAction("DELETE", "no action");
    assertThat(onDeleteNoAction)
        .isEquivalentAccordingToCompareTo(ReferentialAction.ON_DELETE_NO_ACTION);
  }

  @Test
  public void testParsingUnsupportedOnUpdateAction() {
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ReferentialAction.getReferentialAction("UPDATE", "no action"));
    assertThat(exception.getMessage())
        .matches("ON UPDATE referential action not supported: no action");
  }

  @Test
  public void testParsingUnsupportedOnDeleteAction() {
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ReferentialAction.getReferentialAction("DELETE", "RESTRICT"));
    assertThat(exception.getMessage())
        .matches("ON DELETE referential action not supported: RESTRICT");
  }
}
