/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spanner.ddl;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import org.junit.Test;

/**
 * Unit tests for DdlUtilityComponents class. This test passes in multiple events to the
 * DdlUtilityComponents class and validates the output.
 */
public class DdlUtilityComponentsTest {
  @Test
  public void verifyIdentifierQuote() {
    String identifierQuote = DdlUtilityComponents.identifierQuote(Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(identifierQuote, "`");
    identifierQuote = DdlUtilityComponents.identifierQuote(Dialect.POSTGRESQL);
    assertEquals(identifierQuote, "\"");
  }
}
