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
	"text/template"
	"time"
)

var (
	headerTmpl = template.Must(template.New("header").Funcs(template.FuncMap{
		"currentYear": currentYear,
	}).Parse(headerTmplF))

	variableTmpl = template.Must(template.New("variable").Funcs(template.FuncMap{
		"extractAttributeType": extractAttributeType,
	}).Parse(variableTmplF))
)

//go:embed header.tmpl
var headerTmplF string

//go:embed variable.tmpl
var variableTmplF string

func currentYear() string {
	return time.Now().Format("2006")
}

func extractAttributeType(attr *tfjson.SchemaAttribute) string {
	typeName := attr.AttributeType.FriendlyName()
	attrType := attr.AttributeType

	if attrType.IsMapType() {
		typeName = fmt.Sprintf("map(%s)", attr.AttributeType.ElementType().FriendlyName())
	}
	if attrType.IsListType() {
		typeName = fmt.Sprintf("list(%s)", attr.AttributeType.ElementType().FriendlyName())
	}
	if attrType.IsSetType() {
		typeName = fmt.Sprintf("set(%s)", attr.AttributeType.ElementType().FriendlyName())
	}

	return typeName
}
