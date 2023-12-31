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

type And[S Schema] []Matcher[S]
type Or[S Schema] []Matcher[S]

func (and And[S]) Match(name string, data S) bool {
	for _, matcher := range and {
		if !matcher.Match(name, data) {
			return false
		}
	}
	return true
}

func (or Or[S]) Match(name string, data S) bool {
	for _, matcher := range or {
		if matcher.Match(name, data) {
			return true
		}
	}
	return len(or) == 0
}

type Matcher[S Schema] interface {
	Match(name string, data S) bool
}

type MatchName[S Schema] string

func (m MatchName[S]) Match(name string, _ S) bool {
	return name == string(m)
}

type AttrMatcher Matcher[*tfjson.SchemaAttribute]
type MatchIsComputed bool

func (m MatchIsComputed) Match(_ string, data *tfjson.SchemaAttribute) bool {
	if data == nil {
		return false
	}
	return data.Computed == bool(m)
}

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
