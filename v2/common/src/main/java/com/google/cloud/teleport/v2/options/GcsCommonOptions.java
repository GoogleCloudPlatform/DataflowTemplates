package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


/**
 * Common {@link PipelineOptions} for reading/writing data using Google Cloud Storage.
 */
public class GcsCommonOptions {

  private GcsCommonOptions() {
  }

  /**
   * Provides {@link PipelineOptions} to read records from Google Cloud Storage.
   */
  public interface ReadOptions extends PipelineOptions {

    @Description("GCS filepattern for files in bucket to read data from"
        + "The name should be in the format of "
        + "gs://<bucket-name>/path")
    String getInputGcsFilePattern();

    void setInputGcsFilePattern(String inputGcsFilePattern);


  }

  /**
   * Provides {@link PipelineOptions} to write records to Google Cloud Storage.
   */
  public interface WriteOptions extends PipelineOptions {

    @Description("GCS directory in bucket to write data to"
        + "The name should be in the format of "
        + "gs://<bucket-name>/path/to/folder")
    String getOutputGcsDirectory();

    void setOutputGcsDirectory(String outputGcsDirectory);

    @Description(
        "The window duration in which data will be written. "
            + "Should be specified only for writing unbounded PCollection case. Defaults to 30s. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

  }

}
