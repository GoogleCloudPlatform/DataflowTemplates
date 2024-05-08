package org.apache.beam.it.gcp.cloudsql;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;

/** Utilities for {@link CloudSqlResourceManager} implementations. */
public final class CloudSqlResourceManagerUtils {
  // Naming restrictions can be found at:
  // mySQL -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/n0rfg6x1shw0ppn1cwhco6yn09f7.htm
  // postgreSQL -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/p1iw263fz6wvnbn1d6nyw71a9sf2.htm
  // oracle -
  // https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/p0t80fm3l1okawn1x3mvo098svir.htm
  //
  // The tightest restrictions were used across all flavors of JDBC for simplicity.
  private static final int MAX_DATABASE_NAME_LENGTH = 30;
  private static final Pattern ILLEGAL_DATABASE_NAME_CHARS = Pattern.compile("[^a-zA-Z0-9_$#]");
  private static final String REPLACE_DATABASE_NAME_CHAR = "_";
  private static final String TIME_FORMAT = "yyyyMMdd_HHmmss";

  private CloudSqlResourceManagerUtils() {}

  /**
   * Generates a JDBC database name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The database name string.
   */
  static String generateDatabaseName(String baseString) {
    baseString = Character.isLetter(baseString.charAt(0)) ? baseString : "d_" + baseString;

    // Take substring of baseString to account for random suffix
    // TODO(polber) - remove with Beam 2.57.0
    int randomSuffixLength = 6;
    baseString =
        baseString.substring(
            0,
            Math.min(
                baseString.length(),
                MAX_DATABASE_NAME_LENGTH
                    - REPLACE_DATABASE_NAME_CHAR.length()
                    - TIME_FORMAT.length()
                    - REPLACE_DATABASE_NAME_CHAR.length()
                    - randomSuffixLength));

    // Add random suffix to avoid collision
    // TODO(polber) - remove with Beam 2.57.0
    return generateResourceId(
            baseString,
            ILLEGAL_DATABASE_NAME_CHARS,
            REPLACE_DATABASE_NAME_CHAR,
            MAX_DATABASE_NAME_LENGTH,
            DateTimeFormatter.ofPattern(TIME_FORMAT))
        + REPLACE_DATABASE_NAME_CHAR
        + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
  }
}
