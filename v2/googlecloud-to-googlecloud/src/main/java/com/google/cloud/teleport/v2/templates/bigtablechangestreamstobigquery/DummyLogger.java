package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;


import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyLogger extends DoFn<ChangeStreamMutation, String> {

  private final static AtomicLong rowCount = new AtomicLong(0);
  private final static AtomicLong lastLogPrinted = new AtomicLong(0);
  private final static Logger LOG = LoggerFactory.getLogger(DummyLogger.class);

  private final static String workerHost = UUID.randomUUID().toString();

  @ProcessElement
  public void processElement(ProcessContext context) {
    ChangeStreamMutation csm  = context.element();
    if (csm == null) {
      LOG.warn("Null ChangeStreamMutation!");
    } else {
      rowCount.incrementAndGet();
    }

    long ts = lastLogPrinted.get();
    if (ts < System.currentTimeMillis() - 60000) {
      LOG.info("Worker: {}, CSM handled: {}", workerHost, rowCount.get());
      lastLogPrinted.set(System.currentTimeMillis());
    }
  }
}
