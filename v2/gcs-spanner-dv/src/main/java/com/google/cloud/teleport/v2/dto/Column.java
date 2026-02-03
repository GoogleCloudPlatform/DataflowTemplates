package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class Column {

  public abstract String getColName();

  public abstract String getColValue();

  public static Builder builder() {
    return new AutoValue_Column.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setColName(String colName);
    public abstract Builder setColValue(String colValue);
    public abstract Column build();
  }
}
