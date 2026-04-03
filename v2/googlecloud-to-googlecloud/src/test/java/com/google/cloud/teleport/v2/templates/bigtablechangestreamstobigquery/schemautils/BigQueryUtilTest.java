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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.TestUtil;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigQueryDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ModType;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Instant;

/** Tests BigQueryUtil. */
@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class BigQueryUtilTest {

  @Test
  public void testDefaultConfigurationSetCell() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());
    Assert.assertFalse(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertFalse(bigQuery.hasIgnoredColumns());
    Assert.assertFalse(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertFalse(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getSetCellMod(getDefaultSourceInfo(), false);
    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(setCell, tableRow));

    Assert.assertEquals("false", tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN, tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.SET_CELL.getCode(), tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER, tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER, tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_VALUE, tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:03:51.243214", tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE, tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_ROWKEY, tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testDefaultConfigurationDeleteCells() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());
    Assert.assertFalse(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertFalse(bigQuery.hasIgnoredColumns());
    Assert.assertFalse(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertFalse(bigQuery.isIgnoredColumnFamily("cf"));

    Mod deleteCells = getDeleteCellsMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(deleteCells, tableRow));

    Assert.assertEquals("false", tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN, tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.DELETE_CELLS.getCode(), tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER, tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER, tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        "1969-12-31 23:59:59.999999",
        tableRow.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:00:00.000001", tableRow.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE, tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_ROWKEY, tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testIgnoreColumnTest() throws Exception {
    BigQueryUtils bigQuery =
        new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("boo", TestUtil.TEST_IGNORED_COLUMN));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily(TestUtil.TEST_IGNORED_COLUMN_FAMILY));

    Mod setIgnoredColumn = getSetIgnoredColumnMod(getDefaultSourceInfo(), true);
    TableRow tableRowNotWritten = new TableRow();

    // Not written!
    Assert.assertFalse(bigQuery.setTableRowFields(setIgnoredColumn, tableRowNotWritten));

    Mod setGoodColumn = getSetIgnoredColumnMod(getDefaultSourceInfo(), false);
    TableRow tableRowWritten = new TableRow();

    // Written!
    Assert.assertTrue(bigQuery.setTableRowFields(setGoodColumn, tableRowWritten));
  }

  @Test
  public void testIgnoreColumnFamilyTest() throws Exception {
    BigQueryUtils bigQuery =
        new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("boo", TestUtil.TEST_IGNORED_COLUMN));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily(TestUtil.TEST_IGNORED_COLUMN_FAMILY));

    Mod deleteFamilyIgnored = getDeleteIgnoredColumnFamily(getDefaultSourceInfo(), true);
    TableRow tableRowNotWritten = new TableRow();

    // Not written!
    Assert.assertFalse(bigQuery.setTableRowFields(deleteFamilyIgnored, tableRowNotWritten));

    Mod deleteFamilyWritten = getDeleteIgnoredColumnFamily(getDefaultSourceInfo(), false);
    TableRow tableRowWritten = new TableRow();

    // Written
    Assert.assertTrue(bigQuery.setTableRowFields(deleteFamilyWritten, tableRowWritten));

    Assert.assertNull(tableRowWritten.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertNull(tableRowWritten.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRowWritten.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.DELETE_FAMILY.getCode(),
        tableRowWritten.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER,
        tableRowWritten.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER,
        tableRowWritten.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertNull(tableRowWritten.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRowWritten.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRowWritten.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRowWritten.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()));
    Assert.assertNull(tableRowWritten.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRowWritten.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE,
        tableRowWritten.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertArrayEquals(
        TestUtil.TEST_ROWKEY.getBytes(),
        (byte[]) tableRowWritten.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testValidateRequiredValuesAreSet() {
    BigQueryUtils bigQuery =
        new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("boo", TestUtil.TEST_IGNORED_COLUMN));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily(TestUtil.TEST_IGNORED_COLUMN_FAMILY));

    Mod setCellsMissingRK = getSetCellMod(getDefaultSourceInfo(), true);
    TableRow tableRowNotWritten = new TableRow();
    try {
      bigQuery.setTableRowFields(setCellsMissingRK, tableRowNotWritten);
      Assert.fail("It should not be possible to process mutation without rowkey set");
    } catch (Exception e) {
      Assert.assertTrue(
          StringUtils.contains(e.getMessage(), "JSONObject[\"ROW_KEY_BYTES\"] is not a string"));
    }
  }

  @Test
  public void testNonUTFCharsetsSetCell() throws Exception {
    BigQueryUtils bigQuery =
        new BigQueryUtils(
            getNonDefaultSourceInfo(), // KOI8-R charset
            getDefaultDestinationInfo()); // String destinations

    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getSetCellModNonUTFChars(getNonDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(setCell, tableRow));

    Assert.assertEquals("false", tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals("Б", tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.SET_CELL.getCode(), tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER, tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER, tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertEquals("Ц", tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:03:51.243214", tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE, tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertEquals("Ф", tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testNonDefaultConfigurationSetCell() throws Exception {
    BigQueryUtils bigQuery =
        new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getSetCellMod(getDefaultSourceInfo(), false);
    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(setCell, tableRow));

    Assert.assertNull(tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN, tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.SET_CELL.getCode(), tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER, tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER, tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertArrayEquals(
        TestUtil.TEST_GOOD_VALUE.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIMESTAMP, tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE, tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertArrayEquals(
        TestUtil.TEST_ROWKEY.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testSetCellWithProtoDecodedValue() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    // Build a simple proto descriptor programmatically
    Descriptor descriptor = buildSimpleMessageDescriptor();

    // Create a ProtoDecoder targeting the test column family and column
    ProtoDecoder protoDecoder =
        new ProtoDecoder(
            descriptor, TestUtil.TEST_GOOD_COLUMN_FAMILY, TestUtil.TEST_GOOD_COLUMN, false);

    // Build a protobuf message matching the descriptor
    DynamicMessage protoMessage =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "test_user")
            .setField(descriptor.findFieldByName("id"), 42)
            .build();
    byte[] protoBytes = protoMessage.toByteArray();

    // Create a SetCell with proto-encoded bytes as the value
    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            ByteString.copyFrom(protoBytes));

    ChangeStreamMutation mutation = mockMutation(false);

    // Create Mod with ProtoDecoder - this should inject TRANSFORMED_VALUE into the JSON
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, protoDecoder, null);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    // The VALUE_STRING should contain the proto-decoded JSON, not the base64-decoded raw bytes.
    // Without preserveFieldNames, JsonFormat.printer() uses camelCase ("userName").
    String valueString = (String) tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName());
    Assert.assertNotNull(valueString);
    Assert.assertTrue(
        "Expected decoded JSON to contain 'userName' (camelCase), got: " + valueString,
        valueString.contains("userName"));
    Assert.assertTrue(
        "Expected decoded JSON to contain '42', got: " + valueString, valueString.contains("42"));
  }

  @Test
  public void testSetCellWithProtoDecodedValuePreserveFieldNames() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    Descriptor descriptor = buildSimpleMessageDescriptor();

    // Create ProtoDecoder with preserveFieldNames=true
    ProtoDecoder protoDecoder =
        new ProtoDecoder(
            descriptor, TestUtil.TEST_GOOD_COLUMN_FAMILY, TestUtil.TEST_GOOD_COLUMN, true);

    DynamicMessage protoMessage =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "alice")
            .setField(descriptor.findFieldByName("id"), 7)
            .build();
    byte[] protoBytes = protoMessage.toByteArray();

    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            ByteString.copyFrom(protoBytes));

    ChangeStreamMutation mutation = mockMutation(false);
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, protoDecoder, null);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    String valueString = (String) tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName());
    Assert.assertNotNull(valueString);
    // With preserveFieldNames, the JSON should use the original proto field name "user_name"
    Assert.assertTrue(
        "Expected preserved field name 'user_name', got: " + valueString,
        valueString.contains("user_name"));
  }

  @Test
  public void testSetCellWithProtoDecoderNonMatchingColumn() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    Descriptor descriptor = buildSimpleMessageDescriptor();

    // ProtoDecoder targets a different column than the SetCell
    ProtoDecoder protoDecoder =
        new ProtoDecoder(descriptor, TestUtil.TEST_GOOD_COLUMN_FAMILY, "other_column", false);

    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            getBytesString(TestUtil.TEST_GOOD_VALUE));

    ChangeStreamMutation mutation = mockMutation(false);
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, protoDecoder, null);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    // Non-matching column: should fall back to standard base64-decoded value
    Assert.assertEquals(
        TestUtil.TEST_GOOD_VALUE, tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
  }

  @Test
  public void testSetCellWithProtoDecoderInvalidBytes() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    Descriptor descriptor = buildSimpleMessageDescriptor();

    ProtoDecoder protoDecoder =
        new ProtoDecoder(
            descriptor, TestUtil.TEST_GOOD_COLUMN_FAMILY, TestUtil.TEST_GOOD_COLUMN, false);

    // Use bytes that are NOT valid protobuf -- ProtoDecoder.decode returns null for invalid bytes,
    // which means TRANSFORMED_VALUE won't be added to the Mod JSON
    byte[] invalidBytes = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};

    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            ByteString.copyFrom(invalidBytes));

    ChangeStreamMutation mutation = mockMutation(false);
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, protoDecoder, null);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    // Invalid proto bytes: decode returns null, so TRANSFORMED_VALUE is not set.
    // The formatter should fall back to base64-decoded string.
    Object valueString = tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName());
    Assert.assertNotNull(valueString);
    // The value should be the base64-decoded representation of the invalid bytes, not a proto JSON
    Assert.assertFalse(
        "Should not contain proto JSON fields", valueString.toString().contains("testUser"));
  }

  @Test
  public void testSetCellWithNullProtoDecoder() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            getBytesString(TestUtil.TEST_GOOD_VALUE));

    ChangeStreamMutation mutation = mockMutation(false);

    // Passing null for both transforms should behave identically to the original constructor
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, null, null);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    Assert.assertEquals(
        TestUtil.TEST_GOOD_VALUE, tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
  }

  /**
   * Builds a simple proto descriptor programmatically for testing. The message has two fields:
   * user_name (string, field 1) and id (int32, field 2).
   */
  private Descriptor buildSimpleMessageDescriptor() throws DescriptorValidationException {
    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("test.proto")
            .setPackage("test")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("SimpleMessage")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("user_name")
                            .setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                            .build())
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("id")
                            .setNumber(2)
                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                            .build())
                    .build())
            .build();

    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
    return fileDescriptor.findMessageTypeByName("SimpleMessage");
  }

  private ChangeStreamMutation mockMutation(boolean noRowkey) {
    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(noRowkey ? null : getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);
    return mutation;
  }

  @Test
  public void testNonDefaultConfigurationDeleteCells() throws Exception {
    BigQueryUtils bigQuery =
        new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily("cf"));

    Mod deleteCellsMod = getDeleteCellsMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(deleteCellsMod, tableRow));

    Assert.assertNull(tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN, tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(
        ModType.DELETE_CELLS.getCode(), tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_CLUSTER, tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals(
        "" + TestUtil.TEST_TIEBREAKER, tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals(
        "1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("-1", tableRow.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()));
    Assert.assertEquals("1", tableRow.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(
        TestUtil.TEST_CBT_TABLE, tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertArrayEquals(
        TestUtil.TEST_ROWKEY.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testSetCellWithBigEndianTimestampTransform() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());

    ValueTransformerRegistry registry =
        ValueTransformerRegistry.parse(
            TestUtil.TEST_GOOD_COLUMN_FAMILY
                + ":"
                + TestUtil.TEST_GOOD_COLUMN
                + ":BIG_ENDIAN_UINT64_TIMESTAMP_MS");

    // 1711900800000L = 2024-03-31T16:00:00Z
    long millis = 1711900800000L;
    byte[] bytes = ByteBuffer.allocate(8).putLong(millis).array();

    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            ByteString.copyFrom(bytes));

    ChangeStreamMutation mutation = mockMutation(false);
    Mod mod = new Mod(getDefaultSourceInfo(), mutation, setCell, null, registry);

    TableRow tableRow = new TableRow();
    Assert.assertTrue(bigQuery.setTableRowFields(mod, tableRow));

    String valueString = (String) tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName());
    Assert.assertNotNull(valueString);
    Assert.assertTrue(
        "Expected timestamp string containing '2024-03-31', got: " + valueString,
        valueString.contains("2024-03-31"));
    Assert.assertTrue(
        "Expected timestamp string containing '16:00:00', got: " + valueString,
        valueString.contains("16:00:00"));
  }

  private Mod getSetCellModNonUTFChars(BigtableSource source) {
    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            TestUtil.TEST_NON_UTF_COLUMN,
            TestUtil.TEST_TIMESTAMP,
            TestUtil.TEST_NON_UTF_VALUE);

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(setCell));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(TestUtil.TEST_NON_UTF_ROWKEY);
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, setCell);
  }

  private Mod getSetCellMod(BigtableSource source, boolean noRowkey) {
    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TestUtil.TEST_TIMESTAMP,
            getBytesString(TestUtil.TEST_GOOD_VALUE));

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(setCell));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(noRowkey ? null : getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, setCell);
  }

  private Mod getSetIgnoredColumnMod(BigtableSource source, boolean ignoredColumn) {
    SetCell setCell =
        SetCell.create(
            TestUtil.TEST_SPECIFIC_COL_TO_IGNORE_FAMILY,
            getBytesString(
                ignoredColumn
                    ? TestUtil.TEST_SPECIFIC_COL_TO_IGNORE
                    : TestUtil.TEST_SPECIFIC_COL_TO_NOT_IGNORE),
            TestUtil.TEST_TIMESTAMP,
            getBytesString(TestUtil.TEST_GOOD_VALUE));

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(setCell));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, setCell);
  }

  private Mod getDeleteIgnoredColumnFamily(BigtableSource source, boolean ignored) {
    DeleteFamily deleteFamily =
        DeleteFamily.create(
            ignored ? TestUtil.TEST_IGNORED_COLUMN_FAMILY : TestUtil.TEST_GOOD_COLUMN_FAMILY);

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(deleteFamily));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, deleteFamily);
  }

  private Mod getDeleteCellsMod(BigtableSource source) {
    DeleteCells deleteCells =
        DeleteCells.create(
            TestUtil.TEST_GOOD_COLUMN_FAMILY,
            getBytesString(TestUtil.TEST_GOOD_COLUMN),
            TimestampRange.create(-1, 1));

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(deleteCells));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, deleteCells);
  }

  private Instant getSimpleTimestamp(long millis) {
    return Instant.ofEpochMilli(millis);
  }

  @NotNull
  private ByteString getBytesString(String val) {
    return ByteString.copyFrom(val.getBytes(Charset.defaultCharset()));
  }

  private ByteString getSimpleRowKey() {
    return getBytesString(TestUtil.TEST_ROWKEY);
  }

  private BigQueryDestination getDefaultDestinationInfo() {
    return new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        false,
        false,
        false,
        null,
        null,
        null);
  }

  private BigtableSource getNonDefaultSourceInfo() {
    return new BigtableSource(
        TestUtil.TEST_CBT_INSTANCE,
        TestUtil.TEST_CBT_TABLE,
        "KOI8-R",
        "cf",
        "*:col,*:badcol,specific:col_to_ignore",
        org.joda.time.Instant.now());
  }

  private BigQueryDestination getNonDefaultDestinationInfo() {
    return new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        true,
        true,
        true,
        "HOUR",
        1000000000L,
        "is_gc");
  }

  private BigtableSource getDefaultSourceInfo() {
    return new BigtableSource(
        TestUtil.TEST_CBT_INSTANCE,
        TestUtil.TEST_CBT_TABLE,
        "UTF-8",
        null,
        null,
        org.joda.time.Instant.now());
  }
}
