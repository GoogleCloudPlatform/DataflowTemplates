package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.cloud.spanner.Type;
import org.json.JSONObject;

/**
 * The {@link TypesUtils} provides methods that converts information schema types to spanner types,
 * extracts data types from Mod, and converts the extracted data types to spanner types.
 */
public class TypesUtils {

  public static Type informationSchemaGoogleSQLTypeToSpannerType(String type) {
    type = cleanInformationSchemaType(type);
    switch (type) {
      case "BOOL":
        return Type.bool();
      case "BYTES":
        return Type.bytes();
      case "DATE":
        return Type.date();
      case "FLOAT64":
        return Type.float64();
      case "INT64":
        return Type.int64();
      case "JSON":
        return Type.json();
      case "NUMERIC":
        return Type.numeric();
      case "STRING":
        return Type.string();
      case "TIMESTAMP":
        return Type.timestamp();
      default:
        if (type.startsWith("ARRAY")) {
          // Get array type, e.g. "ARRAY<STRING>" -> "STRING".
          String spannerArrayType = type.substring(6, type.length() - 1);
          Type itemType = informationSchemaGoogleSQLTypeToSpannerType(spannerArrayType);
          return Type.array(itemType);
        }

        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", type));
    }
  }

  public static Type informationSchemaPostgreSQLTypeToSpannerType(String type) {
    boolean isPostgresArray = isPostgresArray(type);
    String cleanedType = "";
    if (isPostgresArray) {
      cleanedType = type.substring(0, type.length() - 2);
      Type itemType = informationSchemaPostgreSQLTypeToSpannerType(cleanedType);
      return Type.array(itemType);
    } else {
      cleanedType = cleanInformationSchemaType(type);
    }

    switch (cleanedType) {
      case "BOOLEAN":
        return Type.bool();
      case "BYTEA":
        return Type.bytes();
      case "DOUBLE PRECISION":
        return Type.float64();
      case "BIGINT":
        return Type.int64();
      case "DATE":
        return Type.date();
      case "JSONB":
        return Type.pgJsonb();
      case "NUMERIC":
        return Type.pgNumeric();
      case "CHARACTER VARYING":
        return Type.string();
      case "TIMESTAMP WITH TIME ZONE":
        return Type.timestamp();
      case "SPANNER.COMMIT_TIMESTAMP":
        return Type.timestamp();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported Spanner PostgreSQL type: %s", type));
    }
  }

  private static boolean isPostgresArray(String type) {
    return type.endsWith("[]");
  }

  /**
   * Remove the Spanner type length limit, since Spanner doesn't document clearly on the
   * parameterized types like BigQuery does, i.e. BigQuery's docmentation on <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_data_types">Parameterized
   * data types</a>, but Spanner doesn't have a similar one. We might have problem if we transfer
   * the length limit into BigQuery. By removing the length limit, we essentially loose the
   * constraint of data written to BigQuery, and it won't cause errors.
   */
  private static String cleanInformationSchemaType(String type) {
    // Remove type size, e.g. STRING(1024) -> STRING.
    int leftParenthesisIdx = type.indexOf('(');
    if (leftParenthesisIdx != -1) {
      type = type.substring(0, leftParenthesisIdx) + type.substring(type.indexOf(')') + 1);
    }

    // Convert it to upper case.
    return type.toUpperCase();
  }

  // Eg 1: "{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}" -> ARRAY<STRING>
  // Eg 2: "{\"code\":\"STRING\"}" -> STRING
  public static String extractTypeFromTypeCode(JSONObject typeCodeObj) {
    if (!typeCodeObj.has("array_element_type")) {
      return typeCodeObj.getString("code");
    }
    return "ARRAY<"
        + extractTypeFromTypeCode(typeCodeObj.getJSONObject("array_element_type"))
        + ">";
  }
}
