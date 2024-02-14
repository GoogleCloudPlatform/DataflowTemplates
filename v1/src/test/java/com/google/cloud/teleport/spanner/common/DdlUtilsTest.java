package com.google.cloud.teleport.spanner.common;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.DdlUtils;
import org.junit.jupiter.api.Test;

class DdlUtilsTest {

  @Test
  void quoteIdentifier() {
    String quoted = DdlUtils.quoteIdentifier("Schema.Table", Dialect.POSTGRESQL);
    assertEquals("\"Schema\".\"Table\"", quoted);

    quoted = DdlUtils.quoteIdentifier("Default.Table", Dialect.GOOGLE_STANDARD_SQL);
    assertEquals("`Default`.`Table`", quoted);
  }
}