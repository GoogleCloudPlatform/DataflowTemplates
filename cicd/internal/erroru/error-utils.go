/*
 * Copyright (C) 2022 Google LLC
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

package erroru

import (
	"fmt"
)

// Handles combining two errors into one. Either can be nil, and the other will
// be returned. If both are nil, nil is returned.
func CombineErrors(original error, err error) error {
	if original == nil && err == nil {
		return nil
	}
	if original == nil {
		return err
	}
	if err == nil {
		return original
	}
	return fmt.Errorf("%w\n%v", original, err)
}
