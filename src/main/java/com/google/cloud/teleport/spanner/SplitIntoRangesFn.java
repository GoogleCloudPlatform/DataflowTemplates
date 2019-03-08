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

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Splits a table File into a set of {@link FileShard} objects storing the file, tablename and the
 * range offset/size.
 *
 * <p>Based on <code>SplitIntoRangesFn</code> in {@link
 * org.apache.beam.sdk.io.ReadAllViaFileBasedSource}.
 */
@VisibleForTesting
class SplitIntoRangesFn extends DoFn<ReadableFile, FileShard> {
  static final long DEFAULT_BUNDLE_SIZE = 64 * 1024 * 1024L;

  final PCollectionView<Map<String, String>> filenamesToTableNamesMapView;
  private final long desiredBundleSize;

  SplitIntoRangesFn(
      long desiredBundleSize, PCollectionView<Map<String, String>> filenamesToTableNamesMapView) {
    this.filenamesToTableNamesMapView = filenamesToTableNamesMapView;
    this.desiredBundleSize = desiredBundleSize;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws FileNotFoundException {
    Map<String, String> filenamesToTableNamesMap = c.sideInput(filenamesToTableNamesMapView);
    Metadata metadata = c.element().getMetadata();
    String filename = metadata.resourceId().toString();
    String tableName = filenamesToTableNamesMap.get(filename);

    if (tableName == null) {
      throw new FileNotFoundException(
          "Unknown table name for file:" + filename + " in map " + filenamesToTableNamesMap);
    }

    if (!metadata.isReadSeekEfficient()) {
      // Do not shard the file.
      c.output(FileShard.create(tableName, c.element(), new OffsetRange(0, metadata.sizeBytes())));
    } else {
      // Create shards.
      for (OffsetRange range :
          new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSize, 0)) {
        c.output(FileShard.create(tableName, c.element(), range));
      }
    }
  }
}
