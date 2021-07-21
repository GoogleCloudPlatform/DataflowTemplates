package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.bigtable.BigtableIO}.
 */
public final class BigTableCommonOptions {

  private BigTableCommonOptions() {
  }

  /**
   * Provides {@link PipelineOptions} to write records to a BigTable table.
   */
  public interface WriteOptions extends PipelineOptions {

    @Description("Id of the project where the Cloud BigTable instance to write into is located.")
    String getBigTableProjectId();

    void setBigTableProjectId(String bigTableProjectId);

    @Description("Id of the Cloud BigTable instance to write into.")
    String getBigTableInstanceId();

    void setBigTableInstanceId(String bigTableInstanceId);

    @Description("Id of the Cloud BigTable table to write into.")
    String getBigTableTableId();

    void setBigTableTableId(String bigTableTableId);

    @Description("Column name to use as a key in Cloud BigTable.")
    String getBigTableKeyColumnName();

    void setBigTableKeyColumnName(String bigTableKeyColumnName);

    @Description("Column family name to use in Cloud BigTable.")
    String getBigTableColumnFamilyName();

    void setBigTableColumnFamilyName(String bigTableColumnFamilyName);
  }

}
