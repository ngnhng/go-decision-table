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

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

const decimalPrecision = 128

func toBigFloat(raw any) (*big.Float, error) {
	if raw == nil {
		return nil, nil
	}

	switch v := raw.(type) {
	case *big.Float:
		return cloneDecimal(v), nil
	case big.Float:
		return cloneDecimal(&v), nil
	case string:
		trim := strings.TrimSpace(v)
		if trim == "" {
			return nil, fmt.Errorf("cannot convert empty string to decimal")
		}
		f, ok := new(big.Float).SetPrec(decimalPrecision).SetString(trim)
		if !ok {
			return nil, fmt.Errorf("cannot convert %q to decimal", v)
		}
		return f, nil
	case fmt.Stringer:
		return toBigFloat(v.String())
	case int:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case int8:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case int16:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case int32:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case int64:
		return new(big.Float).SetPrec(decimalPrecision).SetInt64(v), nil
	case uint:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case uint8:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case uint16:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case uint32:
		return big.NewFloat(float64(v)).SetPrec(decimalPrecision), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("value %d overflows decimal converter", v)
		}
		return new(big.Float).SetPrec(decimalPrecision).SetInt64(int64(v)), nil
	case float32:
		return new(big.Float).SetPrec(decimalPrecision).SetFloat64(float64(v)), nil
	case float64:
		return new(big.Float).SetPrec(decimalPrecision).SetFloat64(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to decimal", raw)
	}
}

func cloneDecimal(src *big.Float) *big.Float {
	if src == nil {
		return nil
	}
	dst := new(big.Float).SetPrec(src.Prec())
	return dst.Copy(src)
}
