package com.infusionsoft.dataflow.shared;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.transforms.DoFn;

public class EntityToKey extends DoFn<Entity, Key> {

  @ProcessElement
  public void processElement(ProcessContext context) {
    final Entity entity = context.element();

    context.output(entity.getKey());
  }
}
