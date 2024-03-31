package com.google.cloud.teleport.v2.spanner.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

@Data
@AllArgsConstructor
public class GenericResponse {
    GenericRecord responseRow;
    boolean isEventFiltered;
}
