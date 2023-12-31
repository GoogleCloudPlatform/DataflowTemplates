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

package terraform

import (
	"embed"
	_ "embed"
	"fmt"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/zclconf/go-cty/cty"
	"io"
	"text/template"
	"time"
)

const (
	headerTmpl    = "header"
	moduleTmpl    = "module"
	variablesTmpl = "variables"
)

var (
	tmpls = template.Must(template.New("templates").Funcs(template.FuncMap{
		"currentYear":          currentYear,
		"isAttributeTypeNil":   isAttributeTypeNil,
		"extractAttributeType": extractAttributeType,
	}).ParseFS(tmplFS, "*.tmpl"))
)

//go:embed header.tmpl module.tmpl variable.tmpl
var tmplFS embed.FS

// Schema models terraform provider and resource schemas.
type Schema interface {
	*tfjson.ProviderSchema | *tfjson.Schema | *tfjson.SchemaAttribute
}

type Encoder[S Schema] struct {
	tmplName string
}

func (enc *Encoder[S]) Encode(w io.Writer, data map[string]S) error {
	return tmpls.ExecuteTemplate(w, enc.tmplName, data)
}

func currentYear() string {
	return time.Now().Format("2006")
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

func isAttributeTypeNil(attrType *cty.Type) bool {
	return attrType.Equals(cty.NilType)
}
