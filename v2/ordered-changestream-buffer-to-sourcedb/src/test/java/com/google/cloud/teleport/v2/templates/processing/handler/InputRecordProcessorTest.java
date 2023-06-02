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
package com.google.cloud.teleport.v2.templates.processing.handler;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class InputRecordProcessorTest {

  @Test
  public void simpleRecordParsing() throws Exception {

    InputStream stream =
        Channels.newInputStream(
            FileSystems.open(
                FileSystems.matchNewResource("src/test/resources/bufferInput.json", false)));
    String record = IOUtils.toString(stream, StandardCharsets.UTF_8);
    List<String> response = InputRecordProcessor.parseRecord(record);
    assertEquals(response.size(), 5);
    assertEquals(response.get(0), "table3");
    assertEquals(response.get(1), "{\"userId\":\"test1\"}");
    assertEquals(
        response.get(2), "{\"balance\":\"10\",\"hb_shardId\":\"shard1\",\"name\":\"gollum\"}");
    assertEquals(response.get(3), "INSERT");
    assertEquals(response.get(4), "2023-05-31T09:57:12.194991000Z");
  }

  @Test
  public void allDataTypesRecordParsing() throws Exception {

    InputStream stream =
        Channels.newInputStream(
            FileSystems.open(
                FileSystems.matchNewResource(
                    "src/test/resources/bufferInputAllDatatypes.json", false)));
    String record = IOUtils.toString(stream, StandardCharsets.UTF_8);
    List<String> response = InputRecordProcessor.parseRecord(record);
    assertEquals(response.size(), 5);
    assertEquals(response.get(0), "sample_table");
    assertEquals(response.get(1), "{\"id\":\"12\"}");
    assertEquals(
        response.get(2),
        "{\"bigint_column\":\"4444\",\"binary_column\":\"YWJjbGFyZ2U=\",\"blob_column\":\"YWJiaWdj\",\"bool_column\":false,\"char_column\":\"<char_c\",\"date_column\":\"2023-05-18\",\"datetime_column\":\"2023-05-18T12:01:13.088397258Z\",\"decimal_column\":\"444.222\",\"double_column\":42.42,\"enum_column\":\"1\",\"float_column\":4.2,\"longblob_column\":\"YWJsb25nYmxvYmM=\",\"longtext_column\":\"<longtext_column>\",\"mediumblob_column\":\"YWJjbGFyZ2U=\",\"mediumint_column\":\"333\",\"mediumtext_column\":\"<mediumtext_column>\",\"set_column\":[\"1\",\"2\"],\"smallint_column\":\"22\",\"text_column\":\"aaaaaddd\",\"time_column\":\"10:10:10\",\"timestamp_column\":\"2023-05-18T12:01:13.088397258Z\",\"tinyblob_column\":\"YWJj\",\"tinyint_column\":\"1\",\"tinytext_column\":\"<tinytext_column>\",\"varbinary_column\":\"YWJjbGFyZ2U=\",\"varchar_column\":\"abc\",\"year_column\":\"2023\"}");
    assertEquals(response.get(3), "INSERT");
    assertEquals(response.get(4), "2023-05-31T09:57:12.194991000Z");
  }
}
