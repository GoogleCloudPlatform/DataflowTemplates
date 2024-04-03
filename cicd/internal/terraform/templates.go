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
	"strings"
	"text/template"
	"time"
)

const (
	extraFnName   = "extra"
	headerTmpl    = "header"
	moduleTmpl    = "module"
	variablesTmpl = "variables"
)

var (
	funcs = template.FuncMap{
		"currentYear":          currentYear,
		"isAttributeTypeNil":   isAttributeTypeNil,
		"extractAttributeType": extractAttributeType,
		"sanitize":             sanitize,
		extraFnName:            func(string) string { return "" },
	}

	// FreemarkerPreResourceExtra applies Extra template blocks using {{template "freemarker_preresource"}} in a *template.Template.
	FreemarkerPreResourceExtra Extra = &extraImpl{
		key:   "freemarker_preresource",
		value: freemarkerPreResourceExtra,
	}

	// FreemarkerResourceExtra applies Extra template blocks using {{template "freemarker_resource"}} in a *template.Template.
	FreemarkerResourceExtra Extra = &extraImpl{
		key:   "freemarker_resource",
		value: freemarkerResourceExtra,
	}

	// GoogleProviderExtra applies Extra template blocks using {{template "provider"}} in a *template.Template.
	GoogleProviderExtra Extra = &extraImpl{
		key:   "provider",
		value: googleProviderExtra,
	}

	// GoogleProviderBetaExtra applies Extra template blocks using {{template "provider"}} in a *template.Template.
	GoogleProviderBetaExtra Extra = &extraImpl{
		key:   "provider",
		value: googleProviderBetaExtra,
	}

	// TemplatePathClassic applies Extra template blocks using {{template "template_path"}} in a *template.Template.
	TemplatePathClassic Extra = &extraImpl{
		key:   "template_path",
		value: freemarkerTemplatePathClassic,
	}

	// TemplatePathFlex applies Extra template blocks using {{template "template_path"}} in a *template.Template.
	TemplatePathFlex Extra = &extraImpl{
		key:   "template_path",
		value: freemarkerTemplatePathFlex,
	}

	// ProviderAttributeExtra applies Extra attribute assignment to the resource block 'provider' attribute for
	// the generally available terraform provider.
	ProviderAttributeExtra Extra = &extraImpl{
		key:   "provider_attribute",
		value: "provider = google",
	}

	// ProviderBetaAttributeExtra applies Extra attribute assignment to the resource block 'provider' attribute for
	// the beta terraform provider.
	ProviderBetaAttributeExtra Extra = &extraImpl{
		key:   "provider_attribute",
		value: "provider = google-beta",
	}
)

//go:embed header.tmpl module.tmpl variable.tmpl
var tmplFS embed.FS

//go:embed freemarker_preresource_extra.ftl
var freemarkerPreResourceExtra string

//go:embed freemarker_resource_extra.ftl
var freemarkerResourceExtra string

//go:embed provider_google.tmpl
var googleProviderExtra string

//go:embed provider_google_beta.tmpl
var googleProviderBetaExtra string

//go:embed freemarker_template_path_classic.ftl
var freemarkerTemplatePathClassic string

//go:embed freemarker_template_path_flex.ftl
var freemarkerTemplatePathFlex string

// ModuleEncoder instantiates an Encoder for *tfjson.Schema data with optional Extra extras.
func ModuleEncoder(extra ...Extra) *Encoder[*tfjson.Schema] {
	lookup := map[string]string{}
	for _, ex := range extra {
		lookup[ex.Key()] = ex.Value()
	}
	funcs[extraFnName] = func(key string) string {
		if v, ok := lookup[key]; ok {
			return v
		}
		return ""
	}
	tmpls := template.Must(template.New("templates").Funcs(funcs).ParseFS(tmplFS, "*.tmpl"))
	return &Encoder[*tfjson.Schema]{
		tmplName: moduleTmpl,
		tmpls:    tmpls,
	}
}

// Schema models terraform provider and resource schemas.
type Schema interface {
	*tfjson.ProviderSchema | *tfjson.Schema | *tfjson.SchemaAttribute
}

// Encoder encodes a map[string]Schema using a *template.Template.
type Encoder[S Schema] struct {
	tmplName string
	tmpls    *template.Template
}

// Encode a map[string]Schema using a *template.Template.
func (enc *Encoder[S]) Encode(w io.Writer, data map[string]S) error {
	if enc.tmpls == nil {
		enc.tmpls = template.Must(template.New("templates").Funcs(funcs).ParseFS(tmplFS, "*.tmpl"))
	}
	return enc.tmpls.ExecuteTemplate(w, enc.tmplName, data)
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

func sanitize(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "[", " ")
	s = strings.ReplaceAll(s, "]", "")
	s = strings.ReplaceAll(s, `"`, "'")
	return s
}

type Extra interface {
	Key() string
	Value() string
}

type extraImpl struct {
	key   string
	value string
}

func (extra *extraImpl) Key() string {
	return extra.key
}

func (extra *extraImpl) Value() string {
	return extra.value
}
