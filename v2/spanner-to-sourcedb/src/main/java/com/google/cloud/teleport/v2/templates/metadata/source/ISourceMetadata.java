package com.google.cloud.teleport.v2.templates.metadata.source;

import com.google.cloud.teleport.v2.templates.schema.source.SourceSchema;

public interface ISourceMetadata <T>{
    T getMetadata();
}
