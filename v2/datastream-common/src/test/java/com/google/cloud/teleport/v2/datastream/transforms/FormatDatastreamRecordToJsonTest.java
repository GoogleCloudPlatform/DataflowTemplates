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
package com.google.cloud.teleport.v2.datastream.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.datastream.transforms.FormatDatastreamRecordToJson.UnifiedTypesFormatter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for FormatDatastreamRecordToJson function. These check appropriate Avro-to-Json conv. */
@RunWith(JUnit4.class)
public class FormatDatastreamRecordToJsonTest {

  private static final String EVENT_UUID_KEY = "_metadata_uuid";

  private static final String EVENT_DATAFLOW_TIMESTAMP_KEY = "_metadata_dataflow_timestamp";

  private static final String EXPECTED_FIRST_RECORD =
      "{\"LOCATION_ID\":1000.0,\"STREET_ADDRESS\":\"1297 Via Cola di Rie\","
          + "\"POSTAL_CODE\":\"00989\",\"CITY\":\"Roma\",\"STATE_PROVINCE\":null,"
          + "\"COUNTRY_ID\":\"IT\",\"timestamp_with_tz\":\"2022-10-13T14:30:00.000056Z\","
          + "\"update_timestamp_micros\":1705320821122571751,\"update_timestamp_millis\":1705320821122571751,"
          + "\"update_time_micros\":1705320821122571751,\"update_time_millis\":1705320821122571751,"
          + "\"_metadata_stream\":\"projects/596161805475/locations/us-central1/streams/dylan-stream-20200810test2\","
          + "\"_metadata_timestamp\":1597101230,\"_metadata_read_timestamp\":1597101230,"
          + "\"_metadata_read_method\":\"oracle_dump\",\"_metadata_source_type\":\"oracle_dump\","
          + "\"_metadata_deleted\":false,\"_metadata_table\":\"LOCATIONS\","
          + "\"_metadata_change_type\":null,\"_metadata_primary_keys\":null,"
          + "\"_metadata_schema\":\"HR\",\"_metadata_scn\":null,\"_metadata_ssn\":null,"
          + "\"_metadata_rs_id\":null,\"_metadata_tx_id\":null,"
          + "\"_metadata_row_id\":\"AAAEALAAEAAAACdAAB\",\"_metadata_source\":{\"schema\":\"HR\","
          + "\"table\":\"LOCATIONS\",\"database\":\"XE\",\"row_id\":\"AAAEALAAEAAAACdAAB\"}}";

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
          + "\"_metadata_primary_keys\":[\"id\"],"
          + "\"_metadata_schema\":\"user1\","
          + "\"_metadata_log_file\":\"mysql-bin.000025\","
          + "\"_metadata_log_position\":\"78443804\","
          + "\"_metadata_source\":{\"table\":\"numbers\",\"database\":\"user1\","
          + "\"primary_keys\":[\"id\"],\"log_file\":\"mysql-bin.000025\","
          + "\"log_position\":78443804,\"change_type\":\"INSERT\",\"is_deleted\":false}}";

  private static final String EXPECTED_MYSQL_PEOPLE =
      "{\"id\":1,"
          + "\"email\":\"dylan@email.com\","
          + "\"first_name\":\"Dylan\","
          + "\"last_name\":\"Person\","
          + "\"gender\":\"M\","
          + "\"birth_date\":\"2020-01-01T00:00:00Z\","
          + "\"created_at\":\"2020-02-12T00:00:00Z\","
          + "\"datetime_at\":\"2020-02-12T00:00:00Z\","
          + "\"_0col\":1,"
          + "\"timestamp_with_tz\":\"2022-10-13T14:30:00Z\","
          + "\"_metadata_stream\":"
          + "\"projects/269744978479/locations/us-central1/streams/datastream-test-fbefaf33\","
          + "\"_metadata_timestamp\":1623459160,"
          + "\"_metadata_read_timestamp\":1623459161,"
          + "\"_metadata_read_method\":\"mysql-backfill-fulldump\","
          + "\"_metadata_source_type\":\"mysql\","
          + "\"_metadata_deleted\":false,\"_metadata_table\":\"people\","
          + "\"_metadata_change_type\":\"INSERT\","
          + "\"_metadata_primary_keys\":[\"id\"],"
          + "\"_metadata_schema\":\"test\","
          + "\"_metadata_log_file\":null,"
          + "\"_metadata_log_position\":null,"
          + "\"_metadata_source\":{"
          + "\"table\":\"people\","
          + "\"database\":\"test\","
          + "\"primary_keys\":[\"id\"],"
          + "\"log_file\":null,"
          + "\"log_position\":null,"
          + "\"change_type\":\"INSERT\","
          + "\"is_deleted\":false}}";

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
    ObjectMapper mapper = new ObjectMapper();
    JsonNode changeEvent = mapper.readTree(jsonData);
    ((ObjectNode) changeEvent).remove(EVENT_UUID_KEY);
    ((ObjectNode) changeEvent).remove(EVENT_DATAFLOW_TIMESTAMP_KEY);
    assertEquals(EXPECTED_FIRST_RECORD, changeEvent.toString());
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next();
      FormatDatastreamRecordToJson.create().apply(record);
    }
  }

  public void testParseMySQLPeoplePrimaryKeys() throws IOException, URISyntaxException {
    URL resource =
        getClass()
            .getClassLoader()
            .getResource("FormatDatastreamRecordToJsonTest/mysql_people_test.avro");
    File file = new File(resource.toURI());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);

    GenericRecord record = dataFileReader.next();
    String jsonData = FormatDatastreamRecordToJson.create().apply(record).getOriginalPayload();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode changeEvent = mapper.readTree(jsonData);
    ((ObjectNode) changeEvent).remove(EVENT_UUID_KEY);
    ((ObjectNode) changeEvent).remove(EVENT_DATAFLOW_TIMESTAMP_KEY);
    assertEquals(EXPECTED_MYSQL_PEOPLE, changeEvent.toString());

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
    ObjectMapper mapper = new ObjectMapper();
    JsonNode changeEvent = mapper.readTree(jsonData);
    ((ObjectNode) changeEvent).remove(EVENT_UUID_KEY);
    ((ObjectNode) changeEvent).remove(EVENT_DATAFLOW_TIMESTAMP_KEY);
    assertEquals(EXPECTED_NUMERIC_RECORD, changeEvent.toString());
  }

  @Test
  public void testHashRowId_valid() {
    assertEquals(0L, FormatDatastreamRecord.hashRowIdToInt("AAAAAAAA++++++++++"));
    assertEquals(1L, FormatDatastreamRecord.hashRowIdToInt("AAAAAAAA/+++++++++"));
    assertEquals(2L, FormatDatastreamRecord.hashRowIdToInt("ABCDE1230+++++++++"));
    assertEquals(1152921504606846975L, FormatDatastreamRecord.hashRowIdToInt("AAAAAAAAZZZZZZZZZZ"));
  }

  @Test
  public void testHashRowId_invalid() {
    assertEquals(-1L, FormatDatastreamRecord.hashRowIdToInt(""));
    assertEquals(-1L, FormatDatastreamRecord.hashRowIdToInt("ABCD"));
  }

  @Test
  public void testLogicalType_micros() {
    String fieldNameNegativeNumber = "logicDate-0001-01-01";
    String fieldNamePositiveNumber = "logicDate-1981-10-21";

    Schema fieldSchema = Mockito.mock(Schema.class);
    GenericRecord element = Mockito.mock(GenericRecord.class);
    Mockito.when(fieldSchema.getLogicalType()).thenReturn(LogicalTypes.timestampMicros());
    Mockito.when(element.get(fieldNameNegativeNumber)).thenReturn(-62135596800000000L);
    Mockito.when(element.get(fieldNamePositiveNumber)).thenReturn(375191111000000L);
    ObjectNode jsonObject = new ObjectNode(new JsonNodeFactory(true));

    FormatDatastreamRecordToJson.UnifiedTypesFormatter.handleLogicalFieldType(
        fieldNameNegativeNumber, fieldSchema, element, jsonObject);
    assertTrue(jsonObject.get(fieldNameNegativeNumber).asText().equals("0001-01-01T00:00:00Z"));
    FormatDatastreamRecordToJson.UnifiedTypesFormatter.handleLogicalFieldType(
        fieldNamePositiveNumber, fieldSchema, element, jsonObject);
    assertTrue(jsonObject.get(fieldNamePositiveNumber).asText().equals("1981-11-21T11:45:11Z"));
  }

  @Test
  public void testIntervalNano() throws JsonProcessingException {

    ObjectNode objectNode = new ObjectNode(new JsonNodeFactory(true));

    /* Basic Test. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "basic",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(1000L, 1000L, 3890L, 25L, 331L, 12L, 9L),
        objectNode);

    /* Test with any field set as null gets treated as 0. */

    UnifiedTypesFormatter.handleDatastreamRecordType(
        "null_minute",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(1000L, 1000L, 3890L, 25L, null, 12L, 9L),
        objectNode);

    /* Basic test for negative field. */

    UnifiedTypesFormatter.handleDatastreamRecordType(
        "neg_field_basic",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(1000L, -1000L, 3890L, 25L, 31L, 12L, 9L),
        objectNode);

    /* Test that negative nanos subtract from the fractional seconds, for example 12 Seconds -1 Nanos becomes 11.999999991s. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "neg_fractional_seconds",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(1000L, 31L, 3890L, 25L, 31L, 12L, -9L),
        objectNode);

    /* Test 0 interval. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "zero_interval",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(0L, 0L, 0L, 0L, 0L, 0L, 0L),
        objectNode);

    /* Test almost zero interval with only nanos set. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "one_nano_interval",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(0L, 0L, 0L, 0L, 0L, 0L, 1L),
        objectNode);
    /* Test with large values. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "large_values",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(
            2147483647L, 11L, 2147483647L, 2147483647L, 2147483647L, 2147483647L, 999999999L),
        objectNode);

    /* Test with large negative values. */
    UnifiedTypesFormatter.handleDatastreamRecordType(
        "large_negative_values",
        generateIntervalNanosSchema(),
        generateIntervalNanosRecord(
            -2147483647L,
            -11L,
            -2147483647L,
            -2147483647L,
            -2147483647L,
            -2147483647L,
            -999999999L),
        objectNode);
    String expected =
        "{\"basic\":\"P1000Y1000M3890DT30H31M12.000000009S\","
            + "\"null_minute\":\"P1000Y1000M3890DT25H12.000000009S\","
            + "\"neg_field_basic\":\"P1000Y-1000M3890DT25H31M12.000000009S\","
            + "\"neg_fractional_seconds\":\"P1000Y31M3890DT25H31M11.999999991S\","
            + "\"zero_interval\":\"P0D\","
            + "\"one_nano_interval\":\"P0DT0.000000001S\","
            + "\"large_values\":\"P2147483647Y11M2147483647DT2183871564H21M7.999999999S\","
            + "\"large_negative_values\":\"P-2147483647Y-11M-2147483647DT-2183871564H-21M-7.999999999S\"}";
    assertEquals(expected, new ObjectMapper().writeValueAsString(objectNode));
  }

  private GenericRecord generateIntervalNanosRecord(
      Long years, Long months, Long days, Long hours, Long minutes, Long seconds, Long nanos) {

    GenericRecord genericRecord = new GenericData.Record(generateIntervalNanosSchema());
    genericRecord.put("years", years);
    genericRecord.put("months", months);
    genericRecord.put("days", days);
    genericRecord.put("hours", hours);
    genericRecord.put("minutes", minutes);
    genericRecord.put("seconds", seconds);
    genericRecord.put("nanos", nanos);
    return genericRecord;
  }

  private Schema generateIntervalNanosSchema() {

    return SchemaBuilder.builder()
        .record("intervalNano")
        .fields()
        .name("years")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("months")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("days")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("hours")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("minutes")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("seconds")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("nanos")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .endRecord();
  }
}
