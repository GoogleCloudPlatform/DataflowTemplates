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
	"encoding/json"
	tfjson "github.com/hashicorp/terraform-json"
	"io"
	"text/template"
)

type formatter interface {
	Format(schema *tfjson.ProviderSchema) error
}

type jsonFormatter struct {
	w io.Writer
}

func (f *jsonFormatter) apply(enc *Encoder) {
	f.w = enc.w
	enc.formatter = f
}

func (f *jsonFormatter) Format(schema *tfjson.ProviderSchema) error {
	return json.NewEncoder(f.w).Encode(schema)
}

type templateFormatter struct {
	w    io.Writer
	tmpl template.Template
}

func (f *templateFormatter) apply(enc *Encoder) {
	f.w = enc.w
	enc.formatter = f
}

func (f *templateFormatter) Format(schema *tfjson.ProviderSchema) error {
	return f.tmpl.Execute(f.w, schema)
}
