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

import "text/template"

// Option applies features to the Encoder.
type Option interface {
	apply(enc *Encoder)
}

func WithJsonFormat() Option {
	return &jsonFormatter{}
}

func WithTemplateFormat(tmpl template.Template) Option {
	return &templateFormatter{
		tmpl: tmpl,
	}
}

type optionFormatter struct {
	formatter formatter
}

func (opt *optionFormatter) apply(enc *Encoder) {
	enc.formatter = opt.formatter
}
