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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/hcl/v2/hclwrite"
	tfjson "github.com/hashicorp/terraform-json"
	"io"
	"text/template"
)

type Formatter interface {
	Format(schema *tfjson.ProviderSchema) error
}

func WithJsonFormat() Formatter {
	return &jsonFormatter{}
}

type jsonFormatter struct {
	w io.Writer
}

func (f *jsonFormatter) Format(schema *tfjson.ProviderSchema) error {
	return json.NewEncoder(f.w).Encode(schema)
}

func WithTemplateFormat(tmpl *template.Template, exclude ...Matcher) Formatter {
	return &templateFormatter{
		tmpl:       tmpl,
		exclusions: exclude,
	}
}

type templateFormatter struct {
	w          io.Writer
	tmpl       *template.Template
	exclusions []Matcher
}

func (f *templateFormatter) Format(schema *tfjson.ProviderSchema) error {
	if schema == nil {
		return fmt.Errorf("schema is nil")
	}
	buf := bytes.Buffer{}
	if err := f.tmpl.Execute(&buf, schema); err != nil {
		return err
	}
	b := hclwrite.Format(buf.Bytes())
	_, err := f.w.Write(b)
	return err
}
