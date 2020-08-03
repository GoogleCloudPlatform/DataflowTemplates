/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for FormatDatastreamRecordToJson function. These check appropriate Avro-to-Json conv. */
@RunWith(JUnit4.class)
public class FormatDatastreamRecordToJsonTest {

  private static final String EXPECTED_FIRST_RECORD =
      "{\"ID\":100906.0,"
          + "\"UPDATES_NUM\":1.0,"
          + "\"NUMBER_COL\":51954.09,"
          + "\"FLOAT_COL\":0.07961314689343668,"
          + "\"CHAR_COL\":\"E\","
          + "\"VARCHAR2_COL\":\"course\","
          + "\"DATE_COL\":\"1992-06-16T11:55:20Z\","
          + "\"TIMESTAMP_WITH_TIME_ZONE_COL\":\"1982-05-05T00:44:06+08:38\","
          + "\"_metadata_stream\":\"9decd8d4-0bf2-37bf-8fac-7e895d97d41b\","
          + "\"_metadata_timestamp\":1595301737,"
          + "\"_metadata_read_timestamp\":1595301737,"
          + "\"_metadata_deleted\":false,"
          + "\"_metadata_schema\":\"ROOT\","
          + "\"_metadata_table\":\"E2E_XTTR3XLR1ILAR5QI\","
          + "\"_metadata_change_type\":null,"
          + "\"_metadata_row_id\":null,"
          + "\"_metadata_source\":"
          + "{\"schema\":\"ROOT\","
          + "\"table\":\"E2E_XTTR3XLR1ILAR5QI\",\"database\":\"ORCL\"}}";

  @Test
  public void testParseAvroGenRecord() throws IOException, URISyntaxException {
    URL resource = getClass().getClassLoader().getResource(
        "FormatDatastreamRecordToJsonTest/avro_file_ut.avro");
    File file = new File(resource.toURI());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
        file, datumReader);
    GenericRecord record = dataFileReader.next();
    String jsonData = FormatDatastreamRecordToJson.create().apply(record).getOriginalPayload();
    assertEquals(EXPECTED_FIRST_RECORD, jsonData);
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next();
      FormatDatastreamRecordToJson.create().apply(record);
    }
  }

}
