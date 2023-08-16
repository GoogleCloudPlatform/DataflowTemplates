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

import com.google.cloud.storage.Blob;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import java.nio.charset.Charset;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookForChangelogEntryAvroRecord extends CommitTimeAwarePredicate {
  private static final Logger LOG = LoggerFactory.getLogger(LookForChangelogEntryAvroRecord.class);
  private final ChangelogEntry expected;

  public LookForChangelogEntryAvroRecord(ChangelogEntry expected) {
    this.expected = expected;
  }

  @Override
  public boolean test(Blob o) {
    byte[] content = o.getContent();
    LOG.info(
        "Read from GCS file ({}): {}",
        o.asBlobInfo(),
        new String(content, Charset.defaultCharset()));

    try (DataFileReader<ChangelogEntry> reader =
        new DataFileReader<>(
            new SeekableByteArrayInput(content),
            new ReflectDatumReader<>(ReflectData.get().getSchema(ChangelogEntry.class)))) {

      for (ChangelogEntry changelogEntry : reader) {
        Assert.assertEquals(expected.getTimestamp(), changelogEntry.getTimestamp());
        Assert.assertEquals(expected.getIsGC(), changelogEntry.getIsGC());
        Assert.assertEquals(expected.getModType(), changelogEntry.getModType());
        Assert.assertEquals(expected.getRowKey().toString(), changelogEntry.getRowKey().toString());
        Assert.assertEquals(
            expected.getColumnFamily().toString(), changelogEntry.getColumnFamily().toString());
        Assert.assertEquals(
            expected.getLowWatermark(),
            changelogEntry.getLowWatermark()); // Low watermark is not working yet
        Assert.assertEquals(expected.getColumn().toString(), changelogEntry.getColumn().toString());
        Assert.assertTrue(expected.getCommitTimestamp() <= changelogEntry.getCommitTimestamp());
        observeCommitTime(changelogEntry.getCommitTimestamp());
        Assert.assertTrue(changelogEntry.getTieBreaker() >= 0);
        Assert.assertEquals(expected.getTimestampFrom(), changelogEntry.getTimestampFrom());
        Assert.assertEquals(expected.getTimestampFrom(), changelogEntry.getTimestampTo());
        Assert.assertEquals(expected.getValue(), changelogEntry.getValue());
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
