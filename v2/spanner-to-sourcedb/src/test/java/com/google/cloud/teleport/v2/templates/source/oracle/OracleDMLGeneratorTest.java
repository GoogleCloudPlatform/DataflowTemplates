/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.source.oracle;

import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class OracleDMLGeneratorTest {
  @Test(
      expected =
          com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException.class)
  public void testGetDMLStatementThrowsExceptionOnNullSchema() {
    OracleDMLGenerator generator = new OracleDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    // Because the mocked request returns null for SchemaMapper, this should throw exception
    generator.getDMLStatement(request);
  }
}
