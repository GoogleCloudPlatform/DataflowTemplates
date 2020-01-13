package com.google.cloud.teleport.v2.templates;

import java.util.function.Supplier;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Interface for fetching Beam Schema.
 */
public interface BeamSchemaSupplier extends Supplier<Schema> {

}
