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

package main

import (
	"context"
	"fmt"
	tfjson "github.com/hashicorp/terraform-json"
	"log"
)

// getSchema of the Google and Google Beta terraform providers.
func (t *terraform) getSchema(ctx context.Context) error {
	pr, err := t.cli.ProvidersSchema(ctx)
	if err != nil {
		return err
	}

	for k, schema := range pr.Schemas {
		log.Printf("writing %s schema...\n", k)
		if err := writeSchema(k, schema); err != nil {
			return err
		}
	}

	return nil
}

// writeSchema of Dataflow related terraform resources.
func writeSchema(key string, provider *tfjson.ProviderSchema) error {
	outProvider := &tfjson.ProviderSchema{
		ResourceSchemas: make(map[string]*tfjson.Schema),
	}

	w, ok := output[key]
	if !ok {
		return fmt.Errorf("no output mapped from key: %s", key)
	}

	fter, ok := formatterMap[key]
	if !ok {
		return fmt.Errorf("no formatter mapped form key: %s", key)
	}

	resourceDataflowJobSchema, ok := provider.ResourceSchemas[resourceDataflowJob]
	if ok {
		outProvider.ResourceSchemas[resourceDataflowJob] = resourceDataflowJobSchema
	}
	resourceDataflowFlexJobSchema, ok := provider.ResourceSchemas[resourceDataflowFlexTemplateJob]
	if ok {
		outProvider.ResourceSchemas[resourceDataflowFlexTemplateJob] = resourceDataflowFlexJobSchema
	}

	return fter.Format(outProvider, w)
}
