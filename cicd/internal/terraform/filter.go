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

// Filter applies to a map[string]Schema to include data matching a Matcher.
type Filter[S Schema] struct {
	matcher Matcher[S]
}

// NewFilter instantiates a Filter from a Matcher.
func NewFilter[S Schema](matcher Matcher[S]) *Filter[S] {
	return &Filter[S]{
		matcher: matcher,
	}
}

// Apply the Filter's Matcher on the data. Returns a map[string]S that satisfies the Matcher.
func (filter *Filter[S]) Apply(data map[string]S) map[string]S {
	result := map[string]S{}
	for k, v := range data {
		if filter.matcher.Match(k, v) {
			result[k] = v
		}
	}
	return result
}
