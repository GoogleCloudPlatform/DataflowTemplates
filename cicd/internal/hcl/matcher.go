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

import tfjson "github.com/hashicorp/terraform-json"

var (
	MatchIsDeprecated Matcher = &isDeprecated{}
	MatchIsComputed   Matcher = &isComputed{}
)

type Matcher interface {
	Match(name string, attr *tfjson.SchemaAttribute) bool
}

type isDeprecated struct{}

func (matcher *isDeprecated) Match(_ string, attr *tfjson.SchemaAttribute) bool {
	return attr.Deprecated
}

type isComputed struct{}

func (matcher *isComputed) Match(_ string, attr *tfjson.SchemaAttribute) bool {
	return attr.Computed
}

func NameMatcher(name string) Matcher {
	return &isName{
		name: name,
	}
}

type isName struct {
	name string
}

func (matcher *isName) Match(name string, _ *tfjson.SchemaAttribute) bool {
	return matcher.name == name
}
