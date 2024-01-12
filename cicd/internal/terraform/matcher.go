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

import tfjson "github.com/hashicorp/terraform-json"

// And is a Matcher that applies when all of its containing Matcher instances Match.
type And[S Schema] []Matcher[S]

// Or is a Matcher that applies when any of its containing Matcher instances Match.
type Or[S Schema] []Matcher[S]

// Match returns true when a name and Schema Match for all the containing Matcher instances.
func (and And[S]) Match(name string, data S) bool {
	for _, matcher := range and {
		if !matcher.Match(name, data) {
			return false
		}
	}
	return true
}

// Match returns true when a name and Schema Match for any the containing Matcher instances.
func (or Or[S]) Match(name string, data S) bool {
	for _, matcher := range or {
		if matcher.Match(name, data) {
			return true
		}
	}
	return len(or) == 0
}

// Matcher matches against a name and a Schema.
type Matcher[S Schema] interface {

	// Match a name and a Schema.
	Match(name string, data S) bool
}

// MatchName is a Matcher tha only matches a name.
type MatchName[S Schema] string

// Match matches against a name only.
func (m MatchName[S]) Match(name string, _ S) bool {
	return name == string(m)
}

// AttrMatcher is a Matcher for tfjson.SchemaAttribute data.
type AttrMatcher Matcher[*tfjson.SchemaAttribute]

// MatchIsDeprecated is a Matcher for tfjson.SchemaAttribute Deprecated attribute.
type MatchIsDeprecated bool

// Match whether tfjson.SchemaAttribute Deprecated is the bool value of the MatchIsDeprecated receiver.
func (m MatchIsDeprecated) Match(_ string, data *tfjson.SchemaAttribute) bool {
	if data == nil {
		return false
	}
	return data.Deprecated == bool(m)
}

// MatchIsComputed is a Matcher for tfjson.SchemaAttribute Computed attribute.
type MatchIsComputed bool

// Match whether tfjson.SchemaAttribute Computed is the bool value of the MatchIsDeprecated receiver.
func (m MatchIsComputed) Match(_ string, data *tfjson.SchemaAttribute) bool {
	if data == nil {
		return false
	}
	return data.Computed == bool(m)
}

// Not returns is a Matcher that negates its result i.e. not Matcher.
func Not[S Schema](matcher Matcher[S]) Matcher[S] {
	return &matchIsNot[S]{
		matcher: matcher,
	}
}

type matchIsNot[S Schema] struct {
	matcher Matcher[S]
}

func (m matchIsNot[S]) Match(name string, data S) bool {
	return !m.matcher.Match(name, data)
}
