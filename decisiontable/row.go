// Copyright 2025 Nhat-Nguyen Nguyen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decisiontable

import "fmt"

func (r Row) matches(input map[string]any) (bool, error) {
	for _, cell := range r.EvalCells {
		if cell.dataType == "" {
			return false, fmt.Errorf("row %d column %s missing data type metadata", r.Number, cell.Column)
		}
		actual := input[cell.Column]
		match, err := evaluateCell(cell.dataType, cell.Operator, actual, cell.Value)
		if err != nil {
			return false, fmt.Errorf("row %d column %s: %w", r.Number, cell.Column, err)
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func (r Row) materializeReturnValues() map[string]any {
	values := make(map[string]any, len(r.ReturnCells))
	for _, cell := range r.ReturnCells {
		values[cell.Column] = cloneValueForType(cell.Value, cell.dataType)
	}
	return values
}
