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

import "math/big"

func cloneValueForType(v any, dt DataType) any {
	if dt == DataTypeDecimal {
		if dec, ok := v.(*big.Float); ok {
			return cloneDecimal(dec)
		}
	}
	if dt == DataTypeListString || dt == DataTypeListInteger {
		if list, ok := v.([]any); ok {
			return cloneAnySlice(list)
		}
	}
	return v
}

func cloneArbitraryValue(v any) any {
	if dec, ok := v.(*big.Float); ok {
		return cloneDecimal(dec)
	}
	if list, ok := v.([]any); ok {
		return cloneAnySlice(list)
	}
	return v
}

func cloneAnySlice(src []any) []any {
	if src == nil {
		return nil
	}
	dup := make([]any, len(src))
	for i, v := range src {
		dup[i] = cloneArbitraryValue(v)
	}
	return dup
}
