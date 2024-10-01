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

// Package consts_vars holds shared constants and vars among the subcommands.
package consts_vars

import (
	"path"
)

const (
	Google                          = "google"
	GoogleBeta                      = "google-beta"
	hashicorpRegistry               = "registry.terraform.io/hashicorp"
	ResourceDataflowJob             = "google_dataflow_job"
	ResourceDataflowFlexTemplateJob = "google_dataflow_flex_template_job"
)

var (
	ProviderGoogleUri     = path.Join(hashicorpRegistry, Google)
	ProviderGoogleBetaUri = path.Join(hashicorpRegistry, GoogleBeta)
)
