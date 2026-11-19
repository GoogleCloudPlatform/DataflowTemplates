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
import static org.junit.Assert.assertTrue;

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
  public void testPostgresSortFieldsWithIsDeleted() {
    TableRow r1 = new TableRow();
    r1.set("_metadata_source_type", "postgresql");
    r1.set("_metadata_primary_keys", Arrays.asList("id"));
    DatastreamRow row = DatastreamRow.of(r1);
    List<String> sortFields = row.getSortFields(true);

    assertEquals(3, sortFields.size());
    assertEquals("_metadata_timestamp", sortFields.get(0));
    assertEquals("_metadata_lsn", sortFields.get(1));
    assertEquals("_metadata_deleted", sortFields.get(2));
  }

  @Test
  public void testDmlInfoPostgresLsnNormalizationAndOrdering() {
    List<String> orderFields = Arrays.asList("_metadata_timestamp", "_metadata_lsn");

    DmlInfo backfillInfo =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            orderFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "null"),
            "{}");

    DmlInfo insertInfo =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            orderFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/2902D0D0'"),
            "{}");

    DmlInfo updateInfo =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            orderFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/2903CF48'"),
            "{}");

    DmlInfo shorterHexLsn =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            orderFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/9FFFFFF'"),
            "{}");

    DmlInfo longerHexLsn =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            orderFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/10000000'"),
            "{}");

    assertEquals("1758835512-'00000017/2902D0D0'", insertInfo.getOrderByValueString());
    assertEquals("1758835512-'00000017/2903CF48'", updateInfo.getOrderByValueString());
    assertEquals("1758835512-", backfillInfo.getOrderByValueString());
    assertTrue(
        backfillInfo.getOrderByValueString().compareTo(insertInfo.getOrderByValueString()) < 0);
    assertTrue(
        insertInfo.getOrderByValueString().compareTo(updateInfo.getOrderByValueString()) < 0);
    assertTrue(
        shorterHexLsn.getOrderByValueString().compareTo(longerHexLsn.getOrderByValueString()) < 0);
  }

  @Test
  public void testNormalizeLsnValueEdgeCases() {
    // Null, empty, and literal null variants
    assertEquals("", DmlInfo.normalizeLsnValue(null));
    assertEquals("", DmlInfo.normalizeLsnValue(""));
    assertEquals("", DmlInfo.normalizeLsnValue("null"));
    assertEquals("", DmlInfo.normalizeLsnValue("NULL"));
    assertEquals("", DmlInfo.normalizeLsnValue("'null'"));
    assertEquals("", DmlInfo.normalizeLsnValue("'NULL'"));

    // Quoted vs unquoted valid Postgres LSNs
    assertEquals("'00000017/2902D0D0'", DmlInfo.normalizeLsnValue("'17/2902d0d0'"));
    assertEquals("00000017/2902D0D0", DmlInfo.normalizeLsnValue("17/2902d0d0"));

    // Short string (< 2 chars) or mismatched quotes
    assertEquals("'", DmlInfo.normalizeLsnValue("'"));
    assertEquals("'17/2902D0D0", DmlInfo.normalizeLsnValue("'17/2902D0D0"));
    assertEquals("17/2902D0D0'", DmlInfo.normalizeLsnValue("17/2902D0D0'"));

    // Missing slash, leading slash, or trailing slash
    assertEquals("'00000025:000001a8:0001'", DmlInfo.normalizeLsnValue("'00000025:000001a8:0001'"));
    assertEquals("/2902D0D0", DmlInfo.normalizeLsnValue("/2902D0D0"));
    assertEquals("17/", DmlInfo.normalizeLsnValue("17/"));

    // Non-hex segments around slash (NumberFormatException fallback)
    assertEquals("'GHI/2902D0D0'", DmlInfo.normalizeLsnValue("'GHI/2902D0D0'"));
    assertEquals("17/ZZZZZZZZ", DmlInfo.normalizeLsnValue("17/ZZZZZZZZ"));

    // DmlInfo with more orderByValues than orderByFields
    DmlInfo mismatchedFieldsInfo =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            Arrays.asList("_metadata_timestamp"),
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "extra_val"),
            "{}");
    assertEquals("1758835512-extra_val", mismatchedFieldsInfo.getOrderByValueString());
  }

  @Test
  public void testNormalizeSortKeyLegacyStateCompatibilityAndEdgeCases() {
    List<String> pgFields = Arrays.asList("_metadata_timestamp", "_metadata_lsn");
    DmlInfo newEvent =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            pgFields,
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/2902D0D1'"),
            "{}");

    // 1. Legacy unpadded state vs new padded event at the exact same timestamp
    String legacyUnpaddedState = "1758835512-'17/2902D0D0'";
    String normalizedLegacy = newEvent.normalizeSortKey(legacyUnpaddedState);
    assertEquals("1758835512-'00000017/2902D0D0'", normalizedLegacy);
    assertTrue(newEvent.getOrderByValueString().compareTo(normalizedLegacy) > 0);

    // 2. Idempotency when state is already normalized
    assertEquals(normalizedLegacy, newEvent.normalizeSortKey(normalizedLegacy));

    // 3. Legacy backfill state ("1758835512-null") and already-normalized backfill ("1758835512-")
    assertEquals("1758835512-", newEvent.normalizeSortKey("1758835512-null"));
    assertEquals("1758835512-", newEvent.normalizeSortKey("1758835512-"));
    assertTrue(
        newEvent.getOrderByValueString().compareTo(newEvent.normalizeSortKey("1758835512-null"))
            > 0);

    // 4. ISO-8601 timestamp containing hyphens
    assertEquals(
        "'2026-09-25T21:25:12.027Z'-'00000017/2902D0D0'",
        newEvent.normalizeSortKey("'2026-09-25T21:25:12.027Z'-'17/2902D0D0'"));

    // 5. With trailing _metadata_deleted field
    DmlInfo withDeletedEvent =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            Arrays.asList("_metadata_timestamp", "_metadata_lsn", "_metadata_deleted"),
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'17/2902D0D1'", "0"),
            "{}");
    assertEquals(
        "1758835512-'00000017/2902D0D0'-0",
        withDeletedEvent.normalizeSortKey("1758835512-'17/2902D0D0'-0"));
    assertEquals("1758835512--0", withDeletedEvent.normalizeSortKey("1758835512-null-0"));
    assertEquals("1758835512--0", withDeletedEvent.normalizeSortKey("1758835512--0"));
    // Malformed sortKey missing trailing dash
    assertEquals("malformed_no_dash", withDeletedEvent.normalizeSortKey("malformed_no_dash"));

    // 6. Null sortKey, non-LSN orderByFields, lsnFieldIdx == 0, and missing start dash
    assertEquals(null, newEvent.normalizeSortKey(null));
    assertEquals("malformed_no_dash", newEvent.normalizeSortKey("malformed_no_dash"));

    DmlInfo mysqlEvent =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            Arrays.asList("_metadata_timestamp", "_metadata_log_file", "_metadata_log_position"),
            Arrays.asList("20374293074"),
            Arrays.asList("1758835512", "'mysql-bin.000001'", "456"),
            "{}");
    assertEquals(
        "1758835512-'mysql-bin.000001'-456",
        mysqlEvent.normalizeSortKey("1758835512-'mysql-bin.000001'-456"));

    DmlInfo lsnOnlyEvent =
        DmlInfo.of(
            "{}",
            "INSERT ...",
            "foo",
            "datasourceresponse",
            Arrays.asList("datasourceresponseid"),
            Arrays.asList("_metadata_lsn"),
            Arrays.asList("20374293074"),
            Arrays.asList("'17/2902D0D1'"),
            "{}");
    assertEquals("'00000017/2902D0D0'", lsnOnlyEvent.normalizeSortKey("'17/2902D0D0'"));
  }
}
