package com.google.cloud.teleport.v2.neo4j.templates;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.google.common.truth.Truth.assertThat;

@Category(Neo4jCustomTestCategory.class)
public class MinimalServiceAccountIT {

    @Test
    public void test() {
        assertThat(2).isEqualTo(1 + 2);
    }
}
