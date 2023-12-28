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

// Package hcl implements encoding of terraform.
package hcl

import (
	tfjson "github.com/hashicorp/terraform-json"
	"io"
)

type Encoder struct {
	w         io.Writer
	formatter Formatter
}

// NewEncoder instantiates an Encoder. Applies each Option from the variadic parameter to the instantiated Encoder.
// Defaults to a JSON format.
func NewEncoder(w io.Writer, formatter Formatter) *Encoder {
	enc := &Encoder{
		w:         w,
		formatter: formatter,
	}
	return enc
}

// Encode a tfjson.ProviderSchema.
func (enc *Encoder) Encode(schema *tfjson.ProviderSchema) error {
	return enc.formatter.Format(schema)
}
