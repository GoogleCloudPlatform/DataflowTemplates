package com.google.cloud.teleport.v2.transforms;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
@DefaultCoder(AvroCoder.class)
public class AvroDestination {
    public String name;
    public String jsonSchema;

    //Needed for serialization
    public AvroDestination() { }

    public AvroDestination(String name, String jsonSchema) {
        this.name = name;
        this.jsonSchema = jsonSchema;
    }

    public static AvroDestination of(String name, String jsonSchema) {
        return new AvroDestination(name, jsonSchema);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
