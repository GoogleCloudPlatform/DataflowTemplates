package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyBQ extends DoFn<TableRow, String> {

  private final static AtomicLong rowCount = new AtomicLong(0);
  private final static AtomicLong lastLogPrinted = new AtomicLong(0);
  private final static Logger LOG = LoggerFactory.getLogger(DummyBQ.class);

  private final static String workerHost = UUID.randomUUID().toString();

  @ProcessElement
  public void processElement(ProcessContext context) {
    TableRow tRow  = context.element();
    if (tRow == null) {
      LOG.warn("Null TableRow!");
    } else {
      rowCount.incrementAndGet();
    }

    long ts = lastLogPrinted.get();
    if (ts < System.currentTimeMillis() - 60000) {
      LOG.info("Worker: {}, rows handled: {}", workerHost, rowCount.get());
      lastLogPrinted.set(System.currentTimeMillis());
    }
  }
}
