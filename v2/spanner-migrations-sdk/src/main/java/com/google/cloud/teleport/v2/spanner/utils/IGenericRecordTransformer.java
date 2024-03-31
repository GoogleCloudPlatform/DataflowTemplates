package com.google.cloud.teleport.v2.spanner.utils;

public interface IGenericRecordTransformer {
    void init(String customParameters);

    GenericResponse toSpannerRow(GenericRequest request);

    GenericResponse toSourceRow(GenericRequest request);
}
