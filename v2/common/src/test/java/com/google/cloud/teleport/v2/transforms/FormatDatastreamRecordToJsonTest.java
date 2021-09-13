/*
 * Copyright (C) 2020 Google LLC
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
      "{\"LOCATION_ID\":1000.0,"
          + "\"STREET_ADDRESS\":\"1297 Via Cola di Rie\","
          + "\"POSTAL_CODE\":\"00989\","
          + "\"CITY\":\"Roma\","
          + "\"STATE_PROVINCE\":null,"
          + "\"COUNTRY_ID\":\"IT\","
          + "\"_metadata_stream\":\"projects/596161805475/locations/us-central1/streams/dylan-stream-20200810test2\","
          + "\"_metadata_timestamp\":1597101230,"
          + "\"_metadata_read_timestamp\":1597101230,"
          + "\"_metadata_read_method\":\"oracle_dump\","
          + "\"_metadata_source_type\":\"oracle_dump\","
          + "\"_metadata_deleted\":false,"
          + "\"_metadata_table\":\"LOCATIONS\","
          + "\"_metadata_change_type\":null,"
          + "\"_metadata_schema\":\"HR\","
          + "\"_metadata_row_id\":\"AAAEALAAEAAAACdAAB\","
          + "\"_metadata_scn\":null,"
          + "\"_metadata_ssn\":null,"
          + "\"_metadata_rs_id\":null,"
          + "\"_metadata_tx_id\":null,"
          + "\"_metadata_source\":{\"schema\":\"HR\","
          + "\"table\":\"LOCATIONS\","
          + "\"database\":\"XE\","
          + "\"row_id\":\"AAAEALAAEAAAACdAAB\"}}";

  private static final String EXPECTED_NUMERIC_RECORD =
      "{\"id\":2,\"bitty\":0,\"booly\":0,\"tiny\":-1,\"small\":-1,\"medium\":-1,"
          + "\"inty\":-1,\"big\":-1,\"floater\":1.2,\"doubler\":1.3,"
          + "\"decimaler\":\"11.22\",\"tinyu\":255,\"smallu\":65535,\"mediumu\":16777215,"
          + "\"intyu\":4294967295,\"bigu\":\"0\","
          + "\"_metadata_stream\":\"projects/545418958905/locations/us-central1/streams/stream31\","
          + "\"_metadata_timestamp\":1628184913,"
          + "\"_metadata_read_timestamp\":1628184913,"
          + "\"_metadata_read_method\":\"mysql-cdc-binlog\","
          + "\"_metadata_source_type\":\"mysql\","
          + "\"_metadata_deleted\":false,"
          + "\"_metadata_table\":\"numbers\","
          + "\"_metadata_change_type\":\"INSERT\","
          + "\"_metadata_schema\":\"user1\","
          + "\"_metadata_log_file\":\"mysql-bin.000025\","
          + "\"_metadata_log_position\":\"78443804\","
          + "\"_metadata_source\":{\"table\":\"numbers\",\"database\":\"user1\","
          + "\"primary_keys\":[\"id\"],\"log_file\":\"mysql-bin.000025\","
          + "\"log_position\":78443804,\"change_type\":\"INSERT\",\"is_deleted\":false}}";

  @Test
  public void testParseAvroGenRecord() throws IOException, URISyntaxException {
    URL resource =
        getClass()
            .getClassLoader()
            .getResource("FormatDatastreamRecordToJsonTest/avro_file_ut.avro");
    File file = new File(resource.toURI());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
    GenericRecord record = dataFileReader.next();
    String jsonData = FormatDatastreamRecordToJson.create().apply(record).getOriginalPayload();
    assertEquals(EXPECTED_FIRST_RECORD, jsonData);
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next();
      FormatDatastreamRecordToJson.create().apply(record);
    }
  }

  public void testParseMySQLNumbers() throws IOException, URISyntaxException {
    URL resource =
        getClass()
            .getClassLoader()
            .getResource("FormatDatastreamRecordToJsonTest/mysql_numbers_test.avro");
    File file = new File(resource.toURI());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
    // mysql_numbers_test.avro has 2 records. We are interested in testing the second record
    dataFileReader.next();
    GenericRecord record = dataFileReader.next();
    String jsonData = FormatDatastreamRecordToJson.create().apply(record).getOriginalPayload();
    assertEquals(EXPECTED_NUMERIC_RECORD, jsonData);
  }
}
