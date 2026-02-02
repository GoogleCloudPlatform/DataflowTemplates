package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

@AutoValue
public abstract class SpannerTableReadConfiguration {

  public abstract String tableName();
  public abstract ImmutableList<String> columnToInclude();
  public abstract ImmutableList<String> columnToExclude();
  public abstract String customQuery();

  public static Builder builder() {
    return new AutoValue_SpannerTableReadConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder tableName(String tableName);
    public abstract Builder columnToInclude(ImmutableList<String> columnToInclude);
    public abstract Builder columnToExclude(ImmutableList<String> columnToExclude);
    public abstract Builder customQuery(String customQuery);

    public abstract SpannerTableReadConfiguration build();

  }
}
