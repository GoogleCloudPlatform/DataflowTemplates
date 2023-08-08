/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j.model.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Transform metadata. */
public class Transform implements Serializable {

  private String sql = "";
  private List<Aggregation> aggregations = new ArrayList<>();
  private boolean group;
  private String orderBy = "";
  private String where = "";
  private int limit = -1;

  public String getSql() {
    return sql;
  }

  public List<Aggregation> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<Aggregation> aggregations) {
    this.aggregations = aggregations;
  }

  public boolean isGroup() {
    return group;
  }

  public void setGroup(boolean group) {
    this.group = group;
  }

  public String getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(String orderBy) {
    this.orderBy = orderBy;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public boolean isDefault() {
    return sql.isEmpty()
        && aggregations.isEmpty()
        && !group
        && orderBy.isEmpty()
        && where.isEmpty()
        && limit == -1;
  }
}
