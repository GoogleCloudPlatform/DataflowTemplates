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
package com.google.cloud.syndeo.transforms.datagenerator;

import static org.junit.Assert.assertNotNull;

import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RecordCreatorTest {

  private static final String schemaString =
      "{\"type\":\"record\",\"name\":\"user_info_flat\",\"namespace\":\"com.google.syndeo\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"username\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"10\"},{\"name\":\"age\",\"type\":\"long\",\"default\":0},{\"name\":\"introduction\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"100\"},{\"name\":\"street\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"city\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"state\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"country\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"15\"}]}]}";

  @Test
  public void test() {
    Schema schema = Schema.parse(schemaString);
    Row row = RecordCreator.createRowRecord(schema);
    assertNotNull(row);
  }
}
