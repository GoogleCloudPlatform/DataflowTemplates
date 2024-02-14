package com.google.cloud.teleport.spanner.ddl;

import static com.google.cloud.teleport.spanner.common.DdlUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import java.io.IOException;
import java.io.Serializable;

@AutoValue
public abstract class NamedSchema implements Serializable {

  private static final long serialVersionUID = -5156046721891763991L;

  public abstract String name();

  public abstract Dialect dialect();

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE SCHEMA ").append(quoteIdentifier(name(), dialect()));
  }

  @Override
  public String toString() {
    return prettyPrint();
  }

  public static Builder builder() {
    return new AutoValue_NamedSchema.Builder();
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_NamedSchema.Builder().dialect(dialect);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder name(String value);

    public abstract Builder dialect(Dialect value);

    public abstract NamedSchema build();
  }
}
