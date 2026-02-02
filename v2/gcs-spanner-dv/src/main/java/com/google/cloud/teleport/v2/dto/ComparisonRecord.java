package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.commons.codec.binary.Base64;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ComparisonRecord {

  public abstract String getHash();

  public static Builder builder() {
    return new AutoValue_ComparisonRecord.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setHash(String hash);

    public abstract ComparisonRecord build();
  }

  public static ComparisonRecord fromSpannerStruct(Struct spannerStruct) {
    int nCols = spannerStruct.getColumnCount();
    StringBuilder sbConcatCols = new StringBuilder();
    for (int i = 0; i < nCols; i++) {
      Type colType = spannerStruct.getColumnType(i);

      switch (colType.toString()) {
        case "STRING":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getString(i));
          break;
        case "BYTES":
          sbConcatCols.append(spannerStruct.isNull(i) ? ""
              : Base64.encodeBase64String(spannerStruct.getBytes(i).toByteArray()));
          break;
        case "INT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getLong(i));
          break;
        case "FLOAT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getDouble(i));
          break;
        case "NUMERIC":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBigDecimal(i));
          break;
        case "DATE":
          if (!spannerStruct.isNull(i)) {
            com.google.cloud.Date date = spannerStruct.getDate(i);
            sbConcatCols.append(String.format("%d%d%d",
                date.getYear(),
                date.getMonth(),
                date.getDayOfMonth()));
          }
          break;
        case "BOOL":
        case "BOOLEAN":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBoolean(i));
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type: %s", colType));
      } // switch
    }
    String hash = Hashing.murmur3_128()
        .hashString(sbConcatCols.toString(), StandardCharsets.UTF_8).toString();
    return builder().setHash(hash).build();
  }
}
