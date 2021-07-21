package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.v2.transforms.CsvConverters;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


/**
 * Common {@link PipelineOptions} for reading and writing data in CSV format.
 */
public interface CsvOptions extends PipelineOptions{

  @Description("If file(s) contain headers")
  Boolean getCsvContainsHeaders();

  void setCsvContainsHeaders(Boolean csvContainsHeaders);

  @Description("Delimiting character in CSV. Default: use delimiter provided in csvFormat")
  @Default.InstanceFactory(CsvConverters.DelimiterFactory.class)
  String getCsvDelimiter();

  void setCsvDelimiter(String csvDelimiter);

  @Description(
      "Csv format according to Apache Commons CSV format. Default is: Apache Commons CSV"
          + " default\n"
          + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT\n"
          + "Must match format names exactly found at: "
          + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html")
  @Default.String("Default")
  String getCsvFormat();

  void setCsvFormat(String csvFormat);

  @Description(
      "CSV File encoding format. Default: UTF-8. Allowed Values are US-ASCII"
          + ",ISO-8859-1,UTF-8,UTF-16")
  @Default.String("UTF-8")
  String getCsvFileEncoding();

  void setCsvFileEncoding(String csvFileEncoding);

}
