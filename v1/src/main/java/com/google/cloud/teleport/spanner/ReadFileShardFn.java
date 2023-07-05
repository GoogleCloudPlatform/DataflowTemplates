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
package com.google.cloud.teleport.spanner;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a {@link FileShard} object from the start to end offset, converting the text rows into
 * CSVRecord objects.
 */
@VisibleForTesting
class ReadFileShardFn extends DoFn<FileShard, KV<String, CSVRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFileShardFn.class);

  private final ValueProvider<Character> columnDelimiter;
  private final ValueProvider<Character> fieldQualifier;
  private final ValueProvider<Boolean> trailingDelimiter;
  private final ValueProvider<Character> escape;
  private final ValueProvider<String> nullString;
  private final ValueProvider<Boolean> handleNewLine;

  ReadFileShardFn(
      ValueProvider<Character> columnDelimiter,
      ValueProvider<Character> fieldQualifier,
      ValueProvider<Boolean> trailingDelimiter,
      ValueProvider<Character> escape,
      ValueProvider<String> nullString,
      ValueProvider<Boolean> handleNewLine) {
    this.columnDelimiter = columnDelimiter;
    this.fieldQualifier = fieldQualifier;
    this.trailingDelimiter = trailingDelimiter;
    this.escape = escape;
    this.nullString = nullString;
    this.handleNewLine = handleNewLine;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws FileNotFoundException {
    // CSVFormat
    CSVFormat csvFormat =
        CSVFormat.newFormat(columnDelimiter.get())
            .withQuote(fieldQualifier.get())
            .withIgnoreEmptyLines(true)
            .withTrailingDelimiter(trailingDelimiter.get())
            .withEscape(escape.get())
            .withNullString(nullString.get());

    FileShard shard = c.element();
    ResourceId readingFile = shard.getFile().getMetadata().resourceId();
    if (!handleNewLine.get()) {
      // Create a TextSource, passing null as the delimiter to use the default
      // delimiters ('\n', '\r', or '\r\n').
      TextSource textSource =
          new TextSource(
              shard.getFile().getMetadata(),
              shard.getRange().getFrom(),
              shard.getRange().getTo(),
              null);
      try {
        BoundedSource.BoundedReader<String> reader =
            textSource
                .createForSubrangeOfFile(
                    shard.getFile().getMetadata(),
                    shard.getRange().getFrom(),
                    shard.getRange().getTo())
                .createReader(c.getPipelineOptions());
        for (boolean more = reader.start(); more; more = reader.advance()) {
          Reader in = new StringReader(reader.getCurrent());
          CSVParser parser = new CSVParser(in, csvFormat);
          List<CSVRecord> list = parser.getRecords();
          if (!list.isEmpty()) {
            if (list.size() > 1) {
              throw new RuntimeException("Unable to parse this row: " + c.element());
            }
            CSVRecord record = list.get(0);
            c.output(KV.of(shard.getTableName(), record));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to readFile: " + readingFile, e);
      }
    } else {
      try {
        ReadableByteChannel channel = FileSystems.open(readingFile);
        // Remove log.
        LOG.info(
            "isReadSeekEfficient(): "
                + shard.getFile().getMetadata().isReadSeekEfficient()
                + ". Currently at: "
                + ((SeekableByteChannel) channel).position()
                + ", will move to: "
                + shard.getRange().getFrom()
                + "/"
                + shard.getFile().getMetadata().sizeBytes()
                + ". To: "
                + shard.getRange().getTo()
                + ". Diff: "
                + (shard.getFile().getMetadata().sizeBytes() - shard.getRange().getTo()));
        ((SeekableByteChannel) channel).position(shard.getRange().getFrom());
        InputStream stream = Channels.newInputStream(channel);
        Reader reader = new InputStreamReader(stream);
        CSVParser parser = new CSVParser(reader, csvFormat);
        long recordCount = 0;
        for (CSVRecord record : parser) {
          c.output(KV.of(shard.getTableName(), record));
          recordCount++;
          if (recordCount >= shard.getRecordCount()) {
            parser.close();
            break;
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to readFile: " + readingFile, e);
      }
    }
  }
}
