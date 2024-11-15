package com.google.cloud.teleport.v2.templates.source.common;

import com.google.cloud.teleport.v2.templates.source.sql.SQLConnectionHelper;

import java.util.HashMap;
import java.util.Map;

public class SourceProcessor {

    private final IDMLGenerator dmlGenerator;
    private final Map<String, ISourceDao> sourceDaoMap;
    private final SQLConnectionHelper connectionHelper;

    private SourceProcessor(IDMLGenerator dmlGenerator, Map<String, ISourceDao> sourceDaoMap, SQLConnectionHelper connectionHelper) {
        this.dmlGenerator = dmlGenerator;
        this.sourceDaoMap = sourceDaoMap;
        this.connectionHelper = connectionHelper;
    }

    public IDMLGenerator getDmlGenerator() {
        return dmlGenerator;
    }

    public Map<String, ISourceDao> getSourceDaoMap() {
        return sourceDaoMap;
    }

    public SQLConnectionHelper getConnectionHelper() {
        return connectionHelper;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private IDMLGenerator dmlGenerator;
        private Map<String, ISourceDao> sourceDaoMap = new HashMap<>();
        private SQLConnectionHelper connectionHelper;

        public Builder dmlGenerator(IDMLGenerator dmlGenerator) {
            this.dmlGenerator = dmlGenerator;
            return this;
        }

        public Builder sourceDaoMap(Map<String, ISourceDao> sourceDaoMap) {
            this.sourceDaoMap = sourceDaoMap;
            return this;
        }

        public Builder connectionHelper(SQLConnectionHelper connectionHelper) {
            this.connectionHelper = connectionHelper;
            return this;
        }

        public SourceProcessor build() {
            return new SourceProcessor(dmlGenerator, sourceDaoMap, connectionHelper);
        }
    }
}

