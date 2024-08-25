package com.google.cloud.teleport.lt.dataset.bigquery;

import com.google.auto.value.AutoValue;
import java.util.List;

@AutoValue
public abstract class BigQueryPerfDataset {

  public abstract PerfResultRow latestRow();

  public abstract List<PerfResultRow> previousRows();

  public static BigQueryPerfDataset.Builder builder() {
    return new AutoValue_BigQueryPerfDataset.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract BigQueryPerfDataset.Builder setLatestRow(PerfResultRow latestRow);

    public abstract BigQueryPerfDataset.Builder setPreviousRows(List<PerfResultRow> previousRows);

    abstract BigQueryPerfDataset autoBuild();

    public BigQueryPerfDataset build() {
      return autoBuild();
    }
  }
}
