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

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class DatastreamRowTest {

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
}
