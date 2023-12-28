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

package hcl

import (
	_ "embed"
	"fmt"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/zclconf/go-cty/cty"
	"text/template"
	"time"
)

var (
	headerTmpl = template.Must(template.New("header").Funcs(template.FuncMap{
		"currentYear": currentYear,
	}).Parse(headerTmplF))
)

//go:embed header.tmpl
var headerTmplF string

//go:embed variable.tmpl
var variableTmplF string

func currentYear() string {
	return time.Now().Format("2006")
}

func isAttributeTypeNil(attrType *cty.Type) bool {
	return attrType.Equals(cty.NilType)
}

func extractAttributeType(attrType *cty.Type) string {
	typeName := attrType.FriendlyName()

	if attrType.IsMapType() {
		typeName = fmt.Sprintf("map(%s)", attrType.ElementType().FriendlyName())
	}
	if attrType.IsListType() {
		typeName = fmt.Sprintf("list(%s)", attrType.ElementType().FriendlyName())
	}
	if attrType.IsSetType() {
		typeName = fmt.Sprintf("set(%s)", attrType.ElementType().FriendlyName())
	}

	return typeName
}

func (f *templateFormatter) variableTemplate() *template.Template {
	tmpl := template.Must(f.tmpl.Clone())
	return template.Must(tmpl.Funcs(template.FuncMap{
		"isAttributeTypeNil":   isAttributeTypeNil,
		"extractAttributeType": extractAttributeType,
		"filter":               f.filter,
	}).Parse(variableTmplF))
}

func (f *templateFormatter) filter(input map[string]*tfjson.SchemaAttribute) map[string]*tfjson.SchemaAttribute {
	result := map[string]*tfjson.SchemaAttribute{}
	for name, attr := range input {
		if !f.exclude(name, attr) {
			result[name] = attr
		}
	}
	return result
}

func (f *templateFormatter) exclude(name string, attr *tfjson.SchemaAttribute) bool {
	for _, excl := range f.exclusions {
		if match := excl.Match(name, attr); match {
			return true
		}
	}
	return false
}
