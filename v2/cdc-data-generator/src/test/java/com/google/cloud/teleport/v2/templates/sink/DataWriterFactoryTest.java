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
package com.google.cloud.teleport.v2.templates.sink;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.mysql.MySqlDataWriter;
import com.google.cloud.teleport.v2.templates.spanner.SpannerDataWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Comprehensive unit tests for {@link DataWriterFactory}. */
@RunWith(JUnit4.class)
public class DataWriterFactoryTest {

  @Test
  public void testCreateWriter_mySql() {
    DataWriter writer = DataWriterFactory.createWriter(SinkType.MYSQL, "{}");
    assertNotNull(writer);
    assertTrue(writer instanceof MySqlDataWriter);
  }

  @Test
  public void testCreateWriter_spanner() {
    DataWriter writer = DataWriterFactory.createWriter(SinkType.SPANNER, "{}");
    assertNotNull(writer);
    assertTrue(writer instanceof SpannerDataWriter);
  }

  @Test(expected = NullPointerException.class)
  public void testCreateWriter_unsupportedThrowsException() {
    DataWriterFactory.createWriter(null, "{}");
  }
}
