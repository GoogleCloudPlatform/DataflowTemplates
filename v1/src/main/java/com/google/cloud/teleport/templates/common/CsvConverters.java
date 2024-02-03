/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvConverters {
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(CsvConverters.class);

  /**
   * Gets Csv format according to <a
   * href="https://javadoc.io/doc/org.apache.commons/commons-csv">Apache Commons CSV</a>. If user
   * passed invalid format error is thrown.
   */
  public static ValueProvider<CSVFormat> getCsvFormat(
      ValueProvider<String> formatString, ValueProvider<String> delimiter) {
    return DualInputNestedValueProvider.of(
        formatString,
        delimiter,
        new SerializableFunction<TranslatorInput<String, String>, CSVFormat>() {
          @Override
          public CSVFormat apply(TranslatorInput<String, String> input) {
            CSVFormat format = CSVFormat.Predefined.valueOf(input.getX()).getFormat();
            if (input.getY() != null) {
              return format.withDelimiter(input.getY().charAt(0));
            }
            return format;
          }
        });
  }

  /** Necessary {@link PipelineOptions} options for Csv Pipelines. */
  public interface CsvPipelineOptions extends PipelineOptions {
    @TemplateParameter.Boolean(
        order = 1,
        optional = true,
        description = "Whether input CSV files contain a header record.",
        helpText = "Input CSV files contain a header record (true/false).")
    @Default.Boolean(false)
    ValueProvider<Boolean> getContainsHeaders();

    void setContainsHeaders(ValueProvider<Boolean> containsHeaders);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "Column delimiter of the data files.",
        helpText =
            "The column delimiter of the input text files. Default: use delimiter provided in csvFormat",
        example = ",")
    @Default.InstanceFactory(DelimiterFactory.class)
    ValueProvider<String> getDelimiter();

    void setDelimiter(ValueProvider<String> delimiter);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "CSV Format to use for parsing records.",
        helpText =
            "CSV format specification to use for parsing records. Default is: Default. See https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html for more details. Must match format names exactly found at: "
                + "https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html")
    @Default.String("Default")
    ValueProvider<String> getCsvFormat();

    void setCsvFormat(ValueProvider<String> csvFormat);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"^(US-ASCII|ISO-8859-1|UTF-8|UTF-16)$"},
        description = "CSV file encoding",
        helpText =
            "CSV file character encoding format. Allowed Values are US-ASCII"
                + ", ISO-8859-1, UTF-8, UTF-16")
    @Default.String("UTF-8")
    ValueProvider<String> getCsvFileEncoding();

    void setCsvFileEncoding(ValueProvider<String> csvFileEncoding);
  }

  /**
   * Default value factory to get delimiter from Csv format so that if the user does not pass one
   * in, it matches the supplied {@link CsvPipelineOptions#getCsvFormat()}.
   */
  public static class DelimiterFactory implements DefaultValueFactory<ValueProvider<String>> {

    @Override
    public ValueProvider<String> create(PipelineOptions options) {
      return NestedValueProvider.of(
          options.as(CsvPipelineOptions.class).getCsvFormat(),
          csvFormat -> {
            CSVFormat format = CSVFormat.Predefined.valueOf(csvFormat).getFormat();
            return String.valueOf(format.getDelimiter());
          });
    }
  }

  /**
   * The {@link ReadCsv} class is a {@link PTransform} that reads from one for more Csv files. The
   * transform returns a {@link PCollectionTuple} consisting of the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link ReadCsv#headerTag()} - Contains headers found in files if read with headers,
   *       contains empty {@link PCollection} if no headers.
   *   <li>{@link ReadCsv#lineTag()} - Contains Csv lines as a {@link PCollection} of strings.
   * </ul>
   */
  @AutoValue
  public abstract static class ReadCsv extends PTransform<PBegin, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_CsvConverters_ReadCsv.Builder();
    }

    public abstract ValueProvider<String> csvFormat();

    public abstract ValueProvider<String> delimiter();

    public abstract ValueProvider<Boolean> hasHeaders();

    public abstract ValueProvider<String> inputFileSpec();

    public abstract TupleTag<String> headerTag();

    public abstract TupleTag<String> lineTag();

    public abstract ValueProvider<String> fileEncoding();

    @Override
    public PCollectionTuple expand(PBegin input) {
      return input
          .apply("MatchFilePattern", FileIO.match().filepattern(inputFileSpec()))
          .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.AUTO))
          .apply(
              "ReadCsvWithHeaders",
              ParDo.of(
                      new GetCsvRowsFn(
                          headerTag(),
                          lineTag(),
                          csvFormat(),
                          delimiter(),
                          fileEncoding(),
                          hasHeaders()))
                  .withOutputTags(headerTag(), TupleTagList.of(lineTag())));
    }

    /** Builder for {@link ReadCsv}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setCsvFormat(ValueProvider<String> csvFormat);

      public abstract Builder setDelimiter(ValueProvider<String> delimiter);

      public abstract Builder setHasHeaders(ValueProvider<Boolean> hasHeaders);

      public abstract Builder setInputFileSpec(ValueProvider<String> inputFileSpec);

      public abstract Builder setHeaderTag(TupleTag<String> headerTag);

      public abstract Builder setLineTag(TupleTag<String> lineTag);

      public abstract Builder setFileEncoding(ValueProvider<String> fileEncoding);

      abstract ReadCsv autoBuild();

      public ReadCsv build() {

        return autoBuild();
      }
    }
  }

  /** The {@link GetCsvRowsFn} class gets each row of a Csv file and outputs it as a string. */
  static class GetCsvRowsFn extends DoFn<ReadableFile, String> {

    private final TupleTag<String> headerTag;
    private final TupleTag<String> linesTag;
    private ValueProvider<CSVFormat> csvFormat;
    private final ValueProvider<String> fileEncoding;
    private final ValueProvider<String> delimiter;
    private final ValueProvider<Boolean> hasHeaders;

    GetCsvRowsFn(
        TupleTag<String> headerTag,
        TupleTag<String> linesTag,
        ValueProvider<String> csvFormat,
        ValueProvider<String> delimiter,
        ValueProvider<String> fileEncoding,
        ValueProvider<Boolean> hasHeaders) {
      this.headerTag = headerTag;
      this.linesTag = linesTag;
      this.csvFormat = getCsvFormat(csvFormat, delimiter);
      this.fileEncoding = fileEncoding;
      this.delimiter = delimiter;
      this.hasHeaders = hasHeaders;
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver outputReceiver) {
      ReadableFile filePath = context.element();
      try {
        BufferedReader bufferedReader =
            new BufferedReader(
                Channels.newReader(
                    filePath.open(), Charset.forName(this.fileEncoding.get()).name()));

        if (hasHeaders.get()) {
          CSVParser parser =
              CSVParser.parse(bufferedReader, this.csvFormat.get().withFirstRecordAsHeader());
          outputReceiver
              .get(this.headerTag)
              .output(String.join(this.delimiter.get(), parser.getHeaderNames()));
          parser
              .iterator()
              .forEachRemaining(
                  record ->
                      outputReceiver
                          .get(this.linesTag)
                          .output(String.join(this.delimiter.get(), record)));
        } else {
          CSVParser parser = CSVParser.parse(bufferedReader, this.csvFormat.get());
          parser
              .iterator()
              .forEachRemaining(
                  record ->
                      outputReceiver
                          .get(this.linesTag)
                          .output(String.join(this.delimiter.get(), record)));
        }
      } catch (IOException ioe) {
        LOG.error("Headers do not match, consistency cannot be guaranteed");
        throw new RuntimeException("Could not read Csv headers: " + ioe.getMessage());
      }
    }
  }
}
