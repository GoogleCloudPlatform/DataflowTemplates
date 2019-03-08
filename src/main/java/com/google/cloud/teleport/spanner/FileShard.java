/*
 * Copyright (C) 2019 Google Inc.
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

package com.google.cloud.teleport.spanner;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.range.OffsetRange;

/** Helper class storing File, Table name and range to read. */
@VisibleForTesting
@AutoValue
abstract class FileShard {

  abstract String getTableName();

  abstract ReadableFile getFile();

  abstract OffsetRange getRange();

  static FileShard create(String tableName, ReadableFile file, OffsetRange range) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(range);
    return new AutoValue_FileShard(tableName, file, range);
  }

  @Override
  public String toString() {
    return String.format(
        "FileShard(table:%s,file:%s:%s)",
        getTableName(), getFile().getMetadata().resourceId(), getRange().toString());
  }

  /**
   * Encodes/decodes a {@link FileShard}.
   *
   * <p>A custom Coder for this object is required because the {@link ReadableFile} member is not
   * serializable and requires a custom coder.
   */
  static class Coder extends AtomicCoder<FileShard> {
    private static final Coder INSTANCE = new Coder();

    public static Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(FileShard value, OutputStream os) throws IOException {
      StringUtf8Coder.of().encode(value.getTableName(), os);
      ReadableFileCoder.of().encode(value.getFile(), os);
      VarLongCoder.of().encode(value.getRange().getFrom(), os);
      VarLongCoder.of().encode(value.getRange().getTo(), os);
    }

    @Override
    public FileShard decode(InputStream is) throws IOException {
      String tableName = StringUtf8Coder.of().decode(is);
      ReadableFile file = ReadableFileCoder.of().decode(is);
      long from = VarLongCoder.of().decode(is);
      long to = VarLongCoder.of().decode(is);
      return new AutoValue_FileShard(tableName, file, new OffsetRange(from, to));
    }
  }
}
