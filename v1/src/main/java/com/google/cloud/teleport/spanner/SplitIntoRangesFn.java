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
package com.google.cloud.teleport.spanner;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(SplitIntoRangesFn.class);

  final PCollectionView<Map<String, String>> filenamesToTableNamesMapView;
  private final long desiredBundleSize;
  private final ValueProvider<Character> quoteChar;
  private final ValueProvider<Character> columnDelimiter;
  private final ValueProvider<Character> escapeChar;
  private final ValueProvider<Boolean> handleNewLine;

  SplitIntoRangesFn(
      long desiredBundleSize,
      PCollectionView<Map<String, String>> filenamesToTableNamesMapView,
      ValueProvider<Boolean> handleNewLine) {
    this.filenamesToTableNamesMapView = filenamesToTableNamesMapView;
    this.desiredBundleSize = desiredBundleSize;
    this.handleNewLine = handleNewLine;
    // These are not used in legacy mode.
    this.quoteChar = ValueProvider.StaticValueProvider.of('"');
    this.columnDelimiter = ValueProvider.StaticValueProvider.of(',');
    this.escapeChar = ValueProvider.StaticValueProvider.of((char) 0);
  }

  SplitIntoRangesFn(
      long desiredBundleSize,
      PCollectionView<Map<String, String>> filenamesToTableNamesMapView,
      ValueProvider<Character> quoteChar,
      ValueProvider<Character> columnDelimiter,
      ValueProvider<Character> escapeChar,
      ValueProvider<Boolean> handleNewLine) {
    this.filenamesToTableNamesMapView = filenamesToTableNamesMapView;
    this.desiredBundleSize = desiredBundleSize;
    this.quoteChar = quoteChar;
    this.columnDelimiter = columnDelimiter;
    this.escapeChar = escapeChar;
    this.handleNewLine = handleNewLine;
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
      c.output(
          FileShard.create(
              tableName, c.element(), new OffsetRange(0, metadata.sizeBytes()), Long.MAX_VALUE));
      return;
    }
    if (!handleNewLine.get()) {
      // Create shards without parsing.
      for (OffsetRange range :
          new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSize, 0)) {
        // We fill in MAX_VALUE in the record count as it is not needed when not in handleNewLine
        // mode. This mode uses bounded reader to read the file instead of the CSVParser.
        c.output(FileShard.create(tableName, c.element(), range, Long.MAX_VALUE));
      }
      return;
    }
    try {
      ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
      InputStream stream = Channels.newInputStream(channel);
      Reader reader = new InputStreamReader(stream);
      char escape =
          (escapeChar == null || escapeChar.get() == null) ? ((char) 0) : escapeChar.get();

      int data = 0, prevData = 0;
      // We need to store the record count to know when to stop reading the shard as the CSVParser
      // library does not provide a way to use the end of shard position(in bytes) directly.
      long bytesRead = 0, prevMarker = 0, maxShardSize = desiredBundleSize, recordCount = 0;
      boolean quoted = false;
      while (data != -1) {
        data = reader.read();
        bytesRead++;
        // This always reads the first char of the column, which can either be the start of the row
        // or the character after the delimiter.
        // If the first char is a quote, we assume the whole value is going to be quoted to keep
        // the behaviour consistent with the CSV parser.
        quoted = ((char) data == quoteChar.get());
        if (!quoted) {
          // If unquoted, we need to reach the next unescaped delimiter or unescaped newline while
          // skipping all characters. Any quotes found will be treated as part of the data.
          prevData = data;
          if ((char) data == columnDelimiter.get()) {
            // The first character of the column was a delimiter.
            continue;
          }
          if ((char) data == '\n') {
            // The first character of the column was a newline.
            recordCount++;
            if (bytesRead > maxShardSize) {
              c.output(
                  FileShard.create(
                      tableName,
                      c.element(),
                      new OffsetRange(prevMarker, prevMarker + bytesRead),
                      recordCount));
              prevMarker += bytesRead;
              bytesRead = 0;
              recordCount = 0;
            }
            continue;
          }
          while (data != -1) {
            data = reader.read();
            bytesRead++;
            // If prev char is escaped, do nothing.
            if ((char) prevData == escape) {
              prevData = data;
              continue;
            }
            prevData = data;
            // We reached end of the column, we go back to the outer loop and move to the next
            // column.
            if ((char) data == columnDelimiter.get()) {
              break;
            }
            // We reached an unescaped EOL, hence the record ends here.
            if ((char) data == '\n') {
              recordCount++;
              if (bytesRead > maxShardSize) {
                c.output(
                    FileShard.create(
                        tableName,
                        c.element(),
                        new OffsetRange(prevMarker, prevMarker + bytesRead),
                        recordCount));
                prevMarker += bytesRead;
                bytesRead = 0;
                recordCount = 0;
              }
              break;
            }
          }
          // This means we reached a column delimiter, newline or end of file, go back to outer
          // loop.
          continue;
        } else {
          // If the first column character is a quote, we assume the whole data value is quoted. If
          // the quote is followed by another quote, the value is still assumed to be quoted
          // (""abc" is quoted "abc, ""abc would throw an error as it would treat it as character
          // after a closed quote).
          // In the quoted case, we know our value ends when we reach an unescaped quote,
          // after which only whitespaces are allowed.  We throw an error if we find any character
          // other  than a whitespace. We ignore any newlines or delimiters inside quotes.
          while (data != -1) {
            prevData = data;
            data = reader.read();
            bytesRead++;
            // CSV quotes can be escaped via the escape character or the quote itself.
            if ((char) data == escape || (char) data == quoteChar.get()) {
              prevData = data;
              data = reader.read();
              bytesRead++;
              if ((char) data == quoteChar.get()) {
                // Character is quote hence ignore since prev was an escape.
                continue;
              }
              // Next character is not a quote. If prev char was a quote, that denotes the value
              // ended.
              if ((char) prevData == quoteChar.get()) {
                // We now skip to the next delimiter or EOL allowing only
                // whitespaces as valid chars from here.
                while (data != -1) {
                  if ((char) data == columnDelimiter.get()) {
                    prevData = data;
                    break;
                  }
                  if ((char) data == '\n') {
                    recordCount++;
                    if (bytesRead > maxShardSize) {
                      c.output(
                          FileShard.create(
                              tableName,
                              c.element(),
                              new OffsetRange(prevMarker, prevMarker + bytesRead),
                              recordCount));
                      prevMarker += bytesRead;
                      bytesRead = 0;
                      recordCount = 0;
                    }
                    break;
                  }
                  if ((char) data != ' ') {
                    throw new RuntimeException("Found char '" + (char) data + "' outside quote");
                  }
                  prevData = data;
                  data = reader.read();
                  bytesRead++;
                }
                break;
              }
            }
          }
        }
      }
      // If the last record does not have a newline at the end, we need to account for that record
      // as well.
      if ((char) prevData != '\n') {
        recordCount++;
      }
      // Add last shard to output collection only if it is non-empty.
      if (recordCount > 0) {
        // We subtract one in the final offset to account for EOF read where data becomes -1.
        c.output(
            FileShard.create(
                tableName,
                c.element(),
                new OffsetRange(prevMarker, prevMarker + bytesRead - 1),
                recordCount));
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to readFile: " + metadata.resourceId().toString());
    }
  }
}
