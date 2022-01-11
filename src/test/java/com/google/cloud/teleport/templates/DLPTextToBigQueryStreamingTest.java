/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.templates.DLPTextToBigQueryStreaming.CSVReader;
import com.google.cloud.teleport.templates.DLPTextToBigQueryStreaming.TableRowProcessorDoFn;
import com.google.common.base.Charsets;
import com.google.privacy.dlp.v2.Table;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link DLPTextToBigQueryStreaming} class. */
@RunWith(JUnit4.class)
public class DLPTextToBigQueryStreamingTest {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingTest.class);

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String TOKENIZE_FILE = "tokenization_data.csv";
  private static StringBuilder fileContents = new StringBuilder();
  private static final String HEADER_ROW =
      "CardTypeCode,CardTypeFullName,IssuingBank,CardNumber,CardHoldersName,CVVCVV2,IssueDate,ExpiryDate,"
          + "BillingDate,CardPIN,CreditLimit,MultibyteCharacters";
  private static final String CONTENTS_ROW =
      "MC,Master Card,Wells Fargo,E5ssxfuqnGfF36Kk,Jeremy O Wilson,"
          + "NK3,12/2007,12/2008,3,vmFF,19800,鈴木一郎";
  private static String tokenizedFilePath;

  @BeforeClass
  public static void setupClass() throws IOException {

    tokenizedFilePath = tempFolder.newFile(TOKENIZE_FILE).getAbsolutePath();
    fileContents.append(HEADER_ROW + "\n");
    fileContents.append(CONTENTS_ROW);
    Files.write(
        new File(tokenizedFilePath).toPath(), fileContents.toString().getBytes(Charsets.UTF_8));
  }

  @After
  public void cleanupTestEnvironment() {
    tempFolder.delete();
  }

  /**
   * Tests reading from a sample CSV file in chunks and create DLP Table from the contents and
   * process the contents by converting to Table Row.
   */
  @Test
  public void testFileIOToBigQueryStreamingE2E() throws IOException {
    ValueProvider<Integer> batchSize = p.newProvider(10);

    PCollectionView<List<KV<String, List<String>>>> headerMap =
        p.apply(Create.of(KV.of("tokenization_data", Arrays.asList(HEADER_ROW.split(",")))))
            .apply(View.asList());

    PCollection<KV<String, Table>> dlpTable =
        p.apply("Match", FileIO.match().filepattern(tokenizedFilePath))
            .apply("Read File", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply("Add Keys", WithKeys.of(key -> "tokenization_data"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
            .apply(
                "Create DLP Table",
                ParDo.of(new CSVReader(batchSize, headerMap)).withSideInputs(headerMap));

    PAssert.that(dlpTable)
        .satisfies(
            collection -> {
              KV<String, Table> tableData = collection.iterator().next();
              assertThat(tableData.getKey(), is(equalTo("tokenization_data")));
              assertThat(tableData.getValue().getHeadersCount(), is(equalTo(12)));
              assertThat(tableData.getValue().getRowsCount(), is(equalTo(1)));
              return null;
            });

    PCollection<KV<String, TableRow>> tableRowMap =
        dlpTable.apply(ParDo.of(new TableRowProcessorDoFn()).withSideInputs(headerMap));

    PAssert.that(tableRowMap)
        .satisfies(
            collection -> {
              KV<String, TableRow> result = collection.iterator().next();

              assertThat(result.getValue().get("CardTypeCode"), is(equalTo("MC")));
              assertThat(result.getValue().get("CardTypeFullName"), is(equalTo("Master Card")));
              assertThat(result.getValue().get("IssuingBank"), is(equalTo("Wells Fargo")));
              assertThat(result.getValue().get("CardNumber"), is(equalTo("E5ssxfuqnGfF36Kk")));
              assertThat(result.getValue().get("CardHoldersName"), is(equalTo("Jeremy O Wilson")));
              assertThat(result.getValue().get("CVVCVV2"), is(equalTo("NK3")));
              assertThat(result.getValue().get("IssueDate"), is(equalTo("12/2007")));
              assertThat(result.getValue().get("ExpiryDate"), is(equalTo("12/2008")));
              assertThat(result.getValue().get("BillingDate"), is(equalTo("3")));
              assertThat(result.getValue().get("CardPIN"), is(equalTo("vmFF")));
              assertThat(result.getValue().get("CreditLimit"), is(equalTo("19800")));
              assertThat(result.getValue().get("MultibyteCharacters"), is(equalTo("鈴木一郎")));
              return null;
            });
    p.run();
  }
}
