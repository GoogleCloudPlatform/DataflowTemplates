/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.plugin.model.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Representation for the JSON that contains the list of templates within their categories, used by
 * the Templates UI.
 */
public class TemplatesCategoryJsonModel {
  private Collection<TemplatesCategoryJson> categories;

  public Collection<TemplatesCategoryJson> getCategories() {
    return categories;
  }

  public void setCategories(Collection<TemplatesCategoryJson> categories) {
    this.categories = categories;
  }

  public static class TemplatesCategoryJson {
    private TemplatesCategoryJsonItem category;
    private List<TemplatesCategoryJsonTemplate> templates;

    public TemplatesCategoryJson() {}

    public TemplatesCategoryJson(String name, String displayName) {
      this.category = new TemplatesCategoryJsonItem(name, displayName);
      this.templates = new ArrayList<>();
    }

    public TemplatesCategoryJsonItem getCategory() {
      return category;
    }

    public void setCategory(TemplatesCategoryJsonItem category) {
      this.category = category;
    }

    public List<TemplatesCategoryJsonTemplate> getTemplates() {
      if (this.templates == null) {
        this.templates = new ArrayList<>();
      }
      return templates;
    }

    public void setTemplates(List<TemplatesCategoryJsonTemplate> templates) {
      this.templates = templates;
    }
  }

  public static class TemplatesCategoryJsonItem {
    private String name;
    private String displayName;

    public TemplatesCategoryJsonItem() {}

    public TemplatesCategoryJsonItem(String name, String displayName) {
      this.name = name;
      this.displayName = displayName;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }
  }

  public static class TemplatesCategoryJsonTemplate {
    private String name;
    private String displayName;
    private String templateType;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }

    public String getTemplateType() {
      return templateType;
    }

    public void setTemplateType(String templateType) {
      this.templateType = templateType;
    }
  }
}
