package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class FetchSizeCalculatorTest {

  @Test
  public void testGetFetchSize_Defaults() {
    TableConfig config = TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    // Pass a dummy estimated row size
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L);
    assertTrue(fetchSize >= 100);
    assertTrue(fetchSize <= 100_000);
  }

  @Test
  public void testGetFetchSize_Explicit() {
    TableConfig config = TableConfig.builder("t1").setFetchSize(12345).setApproxRowCount(100L).build();
    // Row size doesn't matter for explicit
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 100L);
    assertEquals(12345, fetchSize);
  }

  @Test
  public void testGetFetchSize_ZeroRowSize() {
    TableConfig config = TableConfig.builder("t1").setFetchSize(null).setApproxRowCount(100L).build();
    int fetchSize = FetchSizeCalculator.getFetchSize(config, 0L);
    // Expected to return default fetch size (50_000)
    assertEquals(50_000, fetchSize);
  }
}
