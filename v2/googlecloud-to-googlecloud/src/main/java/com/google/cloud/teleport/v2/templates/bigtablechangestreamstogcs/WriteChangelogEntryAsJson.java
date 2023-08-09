/*
 * Copyright (C) 2022 Google LLC
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

import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.bigtable.ChangelogEntryJson;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.DestinationInfo;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class WriteChangelogEntryAsJson extends SimpleFunction<ChangelogEntry, String> {
  private static final ThreadLocal<Gson> gsonThreadLocal = ThreadLocal.withInitial(Gson::new);
  private final DestinationInfo destination;
  private transient Charset charset;

  public WriteChangelogEntryAsJson(DestinationInfo destination) {
    this.destination = destination;
    this.charset = Charset.forName(destination.getCharsetName());
  }

  @Override
  public String apply(ChangelogEntry record) {
    if (record == null) {
      return null;
    }

    ChangelogEntryJson jsonType = new ChangelogEntryJson();
    jsonType.setColumn(
        bytesToString(record.getColumn(), destination.isColumnQualifierBase64Encoded(), charset));
    jsonType.setModType(record.getModType());
    jsonType.setTimestamp(record.getTimestamp());
    jsonType.setTimestampTo(record.getTimestampTo());
    jsonType.setTimestampFrom(record.getTimestampFrom());
    jsonType.setIsGC(record.getIsGC());
    jsonType.setValue(
        bytesToString(record.getValue(), destination.isValueBase64Encoded(), charset));
    jsonType.setColumnFamily(record.getColumnFamily());
    jsonType.setCommitTimestamp(record.getCommitTimestamp());
    jsonType.setLowWatermark(record.getLowWatermark());
    jsonType.setRowKey(
        bytesToString(record.getRowKey(), destination.isRowkeyBase64Encoded(), charset));
    jsonType.setTieBreaker(record.getTieBreaker());
    return gsonThreadLocal.get().toJson(jsonType, ChangelogEntryJson.class);
  }

  private String bytesToString(ByteBuffer bytes, boolean useBase64, Charset charset) {
    if (bytes == null) {
      return null;
    } else {
      if (useBase64) {
        return charset.decode(Base64.getEncoder().encode(bytes)).toString();
      } else {
        return charset.decode(bytes).toString();
      }
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    charset = Charset.forName(destination.getCharsetName());
  }
}
