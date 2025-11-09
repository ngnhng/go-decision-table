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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrUnknownColumn is returned when a row references a column that was not registered on the table.
	ErrUnknownColumn = errors.New("unknown column")
	// ErrUnsupportedOperator is returned when an operator is not implemented yet.
	ErrUnsupportedOperator = errors.New("unsupported operator")
)

func sanitizeExpectedValue(dt DataType, op OperatorType, raw any) (any, error) {
	switch op {
	case OperatorMatchesRegex:
		if raw == nil {
			return nil, fmt.Errorf("operator MATCHES_REGEX requires a pattern")
		}
		pattern, err := coercePrimitive(DataTypeString, raw)
		if err != nil {
			return nil, err
		}
		str, ok := pattern.(string)
		if !ok {
			return nil, fmt.Errorf("operator MATCHES_REGEX requires string pattern, got %T", pattern)
		}
		re, err := regexp.Compile(str)
		if err != nil {
			return nil, err
		}
		return re, nil
	case OperatorIsNull, OperatorIsNotNull:
		if raw == nil {
			return true, nil
		}
		flag, err := toBool(raw)
		if err != nil {
			return nil, err
		}
		if !flag {
			return nil, fmt.Errorf("operator %s requires value true", op)
		}
		return flag, nil
	}

	if requiresCollectionValue(op) {
		return sanitizeCollection(dt, raw)
	}
	return coercePrimitive(dt, raw)
}

func sanitizeReturnValue(dt DataType, raw any) (any, error) {
	return coercePrimitive(dt, raw)
}

func sanitizeActualValue(dt DataType, actual any) (any, error) {
	return coercePrimitive(dt, actual)
}

func sanitizeActualCollection(dt DataType, actual any) ([]any, error) {
	return sanitizeCollection(dt, actual)
}

func requiresCollectionValue(op OperatorType) bool {
	switch op {
	case OperatorIn,
		OperatorNotIn,
		OperatorAnyContained,
		OperatorNotAnyContained,
		OperatorAllContained,
		OperatorNotAllContained,
		OperatorContainsAll,
		OperatorNotContainsAll:
		return true
	default:
		return false
	}
}

func expectsActualCollection(op OperatorType) bool {
	switch op {
	case OperatorAnyContained,
		OperatorNotAnyContained,
		OperatorAllContained,
		OperatorNotAllContained,
		OperatorContainsAll,
		OperatorNotContainsAll,
		OperatorAllEqual:
		return true
	default:
		return false
	}
}

func coercePrimitive(dt DataType, raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch dt {
	case DataTypeString:
		return fmt.Sprint(raw), nil
	case DataTypeInteger:
		return toInt64(raw)
	case DataTypeDecimal:
		return toBigFloat(raw)
	case DataTypeBoolean:
		return toBool(raw)
	case DataTypeDate:
		return parseISODate(raw)
	case DataTypeDateTime:
		return parseISODateTime(raw)
	case DataTypeListString:
		return coerceList(raw, DataTypeString)
	case DataTypeListInteger:
		return coerceList(raw, DataTypeInteger)
	default:
		return nil, fmt.Errorf("unsupported data type %s", dt)
	}
}

func sanitizeCollection(dt DataType, raw any) ([]any, error) {
	if raw == nil {
		return nil, nil
	}
	values, err := toInterfaceSlice(raw)
	if err != nil {
		return nil, err
	}
	result := make([]any, len(values))
	baseType := elementDataType(dt)
	for i, v := range values {
		sanitized, err := coercePrimitive(baseType, v)
		if err != nil {
			return nil, err
		}
		result[i] = sanitized
	}
	return result, nil
}

func coerceList(raw any, elemType DataType) ([]any, error) {
	values, err := toInterfaceSlice(raw)
	if err != nil {
		return nil, err
	}
	result := make([]any, len(values))
	for i, v := range values {
		sanitized, err := coercePrimitive(elemType, v)
		if err != nil {
			return nil, err
		}
		result[i] = sanitized
	}
	return result, nil
}

func parseISODate(raw any) (time.Time, error) {
	str, err := toTrimmedString(raw)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := time.Parse("2006-01-02", str)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: %w", str, err)
	}
	return parsed, nil
}

func parseISODateTime(raw any) (time.Time, error) {
	str, err := toTrimmedString(raw)
	if err != nil {
		return time.Time{}, err
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339}
	var lastErr error
	for _, layout := range layouts {
		parsed, parseErr := time.Parse(layout, str)
		if parseErr == nil {
			return parsed, nil
		}
		lastErr = parseErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("unable to parse datetime")
	}
	return time.Time{}, fmt.Errorf("invalid datetime %q: %w", str, lastErr)
}

func toTrimmedString(raw any) (string, error) {
	if raw == nil {
		return "", fmt.Errorf("cannot convert nil to string")
	}
	switch v := raw.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return "", fmt.Errorf("cannot convert empty string value")
		}
		return trimmed, nil
	case fmt.Stringer:
		return toTrimmedString(v.String())
	default:
		trimmed := strings.TrimSpace(fmt.Sprint(v))
		if trimmed == "" {
			return "", fmt.Errorf("cannot convert value %v to string", raw)
		}
		return trimmed, nil
	}
}

func elementDataType(dt DataType) DataType {
	switch dt {
	case DataTypeListString:
		return DataTypeString
	case DataTypeListInteger:
		return DataTypeInteger
	default:
		return dt
	}
}

func toInterfaceSlice(raw any) ([]any, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case []any:
		return append([]any(nil), v...), nil
	case []string:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	case []int:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	case []int64:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	case []float64:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	case []bool:
		out := make([]any, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out, nil
	}

	rv := reflect.ValueOf(raw)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("value %T is not a slice or array", raw)
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, nil
}

func toInt64(raw any) (int64, error) {
	switch v := raw.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows int64", v)
		}
		return int64(v), nil
	case float32:
		if math.Trunc(float64(v)) != float64(v) {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, fmt.Errorf("cannot convert empty string to int64")
		}
		return strconv.ParseInt(trimmed, 10, 64)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, err
		}
		if math.Trunc(f) != f {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", raw)
	}
}

func toBool(raw any) (bool, error) {
	switch v := raw.(type) {
	case bool:
		return v, nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return false, fmt.Errorf("cannot convert empty string to bool")
		}
		switch strings.ToLower(trimmed) {
		case "true", "1", "yes", "y":
			return true, nil
		case "false", "0", "no", "n":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", v)
		}
	case int, int8, int16, int32, int64:
		val := reflect.ValueOf(v).Int()
		return val != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		val := reflect.ValueOf(v).Uint()
		return val != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", raw)
	}
}
