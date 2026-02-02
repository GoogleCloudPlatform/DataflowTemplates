package com.google.cloud.teleport.v2.dto;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.codec.binary.Base64;

@DefaultCoder(AvroCoder.class)
public class ComparisonRecord {

  private String hash;

  //Avro coder needs the default constructor.
  public ComparisonRecord() {
  }

  public String getHash() {
    return hash;
  }

  public static ComparisonRecord fromSpannerStruct(Struct spannerStruct) {
    ComparisonRecord comparisonRecord = new ComparisonRecord();
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
    comparisonRecord.hash = Hashing.murmur3_128()
        .hashString(sbConcatCols.toString(), StandardCharsets.UTF_8).toString();
    return comparisonRecord;
  }
}
