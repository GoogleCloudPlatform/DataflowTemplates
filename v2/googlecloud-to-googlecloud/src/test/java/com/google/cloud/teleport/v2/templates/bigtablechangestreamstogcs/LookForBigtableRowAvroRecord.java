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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import static com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.Tools.bbToString;

import com.google.cloud.storage.Blob;
import com.google.cloud.teleport.bigtable.BigtableCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookForBigtableRowAvroRecord extends CommitTimeAwarePredicate {

  private static final Logger LOG = LoggerFactory.getLogger(LookForBigtableRowAvroRecord.class);
  private final ChangelogEntry expected;

  public LookForBigtableRowAvroRecord(ChangelogEntry expected) {
    this.expected = expected;
  }

  @Override
  public boolean test(Blob o) {
    byte[] content = o.getContent();

    try (DataFileReader<BigtableRow> reader =
        new DataFileReader<>(
            new SeekableByteArrayInput(content),
            new ReflectDatumReader<>(ReflectData.get().getSchema(BigtableRow.class)))) {
      for (BigtableRow row : reader) {
        LOG.info("Object read: {}", row);

        for (BigtableCell cell : row.getCells()) {
          String qualifier = bbToString(cell.getQualifier());
          String value = bbToString(cell.getValue());
          long timestamp = cell.getTimestamp();

          // Bigtable rows describing changelog has 0 timestamps
          Assert.assertEquals(0, timestamp);

          switch (qualifier) {
            case "row_key":
              Assert.assertEquals(bbToString(expected.getRowKey()), value);
              break;
            case "mod_type":
              Assert.assertEquals(expected.getModType().toString(), value);
              break;
            case "is_gc":
              Assert.assertEquals(Boolean.toString(expected.getIsGC()), value);
              break;
            case "column":
              Assert.assertEquals(bbToString(expected.getColumn()), value);
              break;
            case "value":
              Assert.assertEquals(bbToString(expected.getValue()), value);
              break;
            case "tiebreaker":
              Assert.assertEquals(expected.getTieBreaker(), Integer.parseInt(value));
              break;
            case "timestamp":
              Assert.assertEquals(expected.getTimestamp(), (Long) Long.parseLong(value));
              break;
            case "column_family":
              Assert.assertEquals(expected.getColumnFamily().toString(), value);
              break;
            case "commit_timestamp":
              observeCommitTime(Long.parseLong(value));
              Assert.assertTrue(expected.getCommitTimestamp() <= Long.parseLong(value));
              break;
            case "low_watermark":
              Assert.assertEquals(expected.getLowWatermark(), Long.parseLong(value));
              break;
            case "timestamp_from":
              Assert.assertEquals(
                  expected.getTimestampFrom(), (value == null ? null : Long.parseLong(value)));
              break;
            case "timestamp_to":
              Assert.assertEquals(
                  expected.getTimestampTo(), (value == null ? null : Long.parseLong(value)));
              break;
            default:
              throw new IllegalStateException("Unexpected column qualifier: " + qualifier);
          }
        }
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
