package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class SpannerTableReadConfiguration {

  public abstract String getTableName();

  @Nullable
  public abstract List<String> getColumnsToInclude();
  @Nullable
  public abstract List<String> getColumnsToExclude();
  @Nullable
  public abstract String getCustomQuery();

  public static Builder builder() {
    return new AutoValue_SpannerTableReadConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableName(String tableName);
    public abstract Builder setColumnsToInclude(List<String> columnsToInclude);
    public abstract Builder setColumnsToExclude(List<String> columnsToExclude);
    public abstract Builder setCustomQuery(String customQuery);

    public abstract SpannerTableReadConfiguration build();

  }
}
