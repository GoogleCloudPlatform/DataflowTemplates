/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.datastream.values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DatastreamRowTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testGetPrimaryKeysAsQuotedString() throws IOException, GeneralSecurityException {
    TableRow r1 = new TableRow();
    r1.set("_metadata_primary_keys", "[\"id\",\"name\"]");
    r1.set("_metadata_source_type", "oracle");
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(pks.size(), 2);
    assertEquals(pks.get(0), "id");
    assertEquals(pks.get(1), "name");
  }

  @Test
  public void testGetPrimaryKeysAsString() throws IOException, GeneralSecurityException {
    TableRow r1 = new TableRow();
    r1.set("_metadata_primary_keys", "[id, name]");
    r1.set("_metadata_source_type", "oracle");
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(pks.size(), 2);
    assertEquals(pks.get(0), "id");
    assertEquals(pks.get(1), "name");
  }

  @Test
  public void testGetPrimaryKeysAsList() throws IOException, GeneralSecurityException {
    TableRow r1 = new TableRow();
    r1.set("_metadata_primary_keys", Arrays.asList(new String[] {"id", "name"}));
    r1.set("_metadata_source_type", "oracle");
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(pks.size(), 2);
    assertEquals(pks.get(0), "id");
    assertEquals(pks.get(1), "name");
  }

  @Test
  public void testSqlServerSortFields() {
    TableRow r1 = new TableRow();
    r1.set("_metadata_source_type", "sqlserver");
    r1.set("_metadata_primary_keys", Arrays.asList("id"));
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> sortFields = row.getSortFields();

    assertEquals(2, sortFields.size());
    assertEquals("_metadata_timestamp", sortFields.get(0));
    assertEquals("_metadata_lsn", sortFields.get(1));
  }

  @Test
  public void testGetPrimaryKeysFromJsonNode_standardPrimaryKeys() throws Exception {
    JsonNode node = MAPPER.readTree("{\"_metadata_primary_keys\": [\"id\", \"dept_id\"]}");
    DatastreamRow row = DatastreamRow.of(node);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(2, pks.size());
    assertEquals("id", pks.get(0));
    assertEquals("dept_id", pks.get(1));
  }

  @Test
  public void testGetPrimaryKeysFromJsonNode_sqlServerReplicationIndex() throws Exception {
    JsonNode node =
        MAPPER.readTree(
            "{\"source_metadata\": {\"replication_index\": [\"emp_id\", \"seq_num\"]}}");
    DatastreamRow row = DatastreamRow.of(node);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(2, pks.size());
    assertEquals("emp_id", pks.get(0));
    assertEquals("seq_num", pks.get(1));
  }

  @Test
  public void testGetPrimaryKeysFromJsonNode_metadataSourceReplicationIndex() throws Exception {
    JsonNode node =
        MAPPER.readTree("{\"_metadata_source\": {\"replication_index\": [\"order_id\"]}}");
    DatastreamRow row = DatastreamRow.of(node);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(1, pks.size());
    assertEquals("order_id", pks.get(0));
  }

  @Test
  public void testGetPrimaryKeysFromJsonNode_sourceMetadataPrimaryKeys() throws Exception {
    JsonNode node = MAPPER.readTree("{\"source_metadata\": {\"primary_keys\": [\"customer_id\"]}}");
    DatastreamRow row = DatastreamRow.of(node);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(1, pks.size());
    assertEquals("customer_id", pks.get(0));
  }

  @Test
  public void testGetPrimaryKeysFromTableRow_replicationIndex() {
    TableRow r1 = new TableRow();
    r1.set("replication_index", Arrays.asList("id", "item_num"));
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(2, pks.size());
    assertEquals("id", pks.get(0));
    assertEquals("item_num", pks.get(1));
  }

  @Test
  public void testGetPrimaryKeysFromTableRow_sourceMetadataReplicationIndex() {
    TableRow r1 = new TableRow();
    Map<String, Object> sourceMeta = new HashMap<>();
    sourceMeta.put("replication_index", Arrays.asList("order_id"));
    r1.set("source_metadata", sourceMeta);
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> pks = row.getPrimaryKeys();

    assertEquals(1, pks.size());
    assertEquals("order_id", pks.get(0));
  }

  @Test
  public void testIsDeleted_jsonNode() throws Exception {
    assertTrue(DatastreamRow.of(MAPPER.readTree("{\"_metadata_deleted\": true}")).isDeleted());
    assertFalse(DatastreamRow.of(MAPPER.readTree("{\"_metadata_deleted\": false}")).isDeleted());
    assertTrue(
        DatastreamRow.of(MAPPER.readTree("{\"_metadata_change_type\": \"DELETE\"}")).isDeleted());
    assertFalse(
        DatastreamRow.of(MAPPER.readTree("{\"_metadata_change_type\": \"INSERT\"}")).isDeleted());
    assertTrue(
        DatastreamRow.of(MAPPER.readTree("{\"source_metadata\": {\"is_deleted\": true}}"))
            .isDeleted());
    assertTrue(
        DatastreamRow.of(MAPPER.readTree("{\"source_metadata\": {\"change_type\": \"DELETE\"}}"))
            .isDeleted());
    assertFalse(
        DatastreamRow.of(MAPPER.readTree("{\"source_metadata\": {\"change_type\": \"UPDATE\"}}"))
            .isDeleted());
  }

  @Test
  public void testIsDeleted_tableRow() {
    TableRow r1 = new TableRow();
    r1.set("_metadata_deleted", true);
    assertTrue(DatastreamRow.of(r1).isDeleted());

    TableRow r2 = new TableRow();
    r2.set("_metadata_deleted", false);
    assertFalse(DatastreamRow.of(r2).isDeleted());

    TableRow r3 = new TableRow();
    r3.set("_metadata_change_type", "DELETE");
    assertTrue(DatastreamRow.of(r3).isDeleted());

    TableRow r4 = new TableRow();
    Map<String, Object> sourceMeta = new HashMap<>();
    sourceMeta.put("change_type", "DELETE");
    r4.set("source_metadata", sourceMeta);
    assertTrue(DatastreamRow.of(r4).isDeleted());
  }
}
