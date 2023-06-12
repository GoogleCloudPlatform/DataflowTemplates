package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountingLogger {

  private final static Logger LOG = LoggerFactory.getLogger(CountingLogger.class);

  private final static AtomicLong BYTES_HANDLED = new AtomicLong(0);

  private final static AtomicLong LAST_LOGGED = new AtomicLong(0);
  private final static String WORKER = UUID.randomUUID().toString();

  public static void addLength(Object value) {
    long increment = 0;
    if (value instanceof String) {
      increment = ((String)value).length();
    } else if (value instanceof byte[]) {
      increment = ((byte[]) value).length;
    }
    synchronized (CountingLogger.class) {
      long newTotal = BYTES_HANDLED.addAndGet(increment);
      long now = System.currentTimeMillis();
      if (LAST_LOGGED.get() < now - 60000) {
        BYTES_HANDLED.set(0);
        LAST_LOGGED.set(now);
        LOG.info("Worker " + WORKER + ", passed bytes to BQ since last time: " + newTotal);
      }
    }
  }
}
