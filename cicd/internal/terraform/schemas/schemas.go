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

// Package schemas contains terraform providers schemas acquired the published registry.
package schemas

import _ "embed"

// Google is the JSON schema of the terraform provider: https://github.com/hashicorp/terraform-provider-google
// It is the output of
// go run ./cmd/run-terraform-command download /path/to/download.json
// and
// go run ./cmd/run-terraform-command describe google google_dataflow_job /path/to/download.json
//
//go:embed google.json
var Google []byte

// GoogleBeta is the JSON schema of the terraform provider: https://github.com/hashicorp/terraform-provider-google-beta
// It is the output of
// go run ./cmd/run-terraform-command download /path/to/download.json
// and
// go run ./cmd/run-terraform-command describe google-beta google_dataflow_flex_template_job /path/to/download.json
//
//go:embed google-beta.json
var GoogleBeta []byte
