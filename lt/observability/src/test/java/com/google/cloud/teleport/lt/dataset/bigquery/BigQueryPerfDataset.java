package com.google.cloud.teleport.lt.dataset.bigquery;

import com.google.auto.value.AutoValue;
import java.util.List;

@AutoValue
public abstract class BigQueryPerfDataset {

  public abstract String query();

  public abstract List<PerfResultRow> rows();

  public static BigQueryPerfDataset.Builder builder() {
    return new AutoValue_BigQueryPerfDataset.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract BigQueryPerfDataset.Builder setRows(List<PerfResultRow> previousRows);

    public abstract BigQueryPerfDataset.Builder setQuery(String query);

    abstract BigQueryPerfDataset autoBuild();

    public BigQueryPerfDataset build() {
      return autoBuild();
    }
  }
}
