package com.google.cloud.teleport.plugin.terraform;

import com.google.auto.value.AutoValue;

import java.util.List;

@AutoValue
public abstract class TerraformModule {

    static Builder builder() {
        return new AutoValue_TerraformModule.Builder();
    }

    public abstract String getName();
    public abstract List<TerraformVariable> getParameters();

    @AutoValue.Builder
    abstract static class Builder {

        abstract Builder setName(String value);

        abstract Builder setParameters(List<TerraformVariable> value);

        abstract TerraformModule build();
    }
}
