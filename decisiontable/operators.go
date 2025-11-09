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
	"math/big"
	"regexp"
	"time"
)

func evaluateCell(dt DataType, op OperatorType, actual any, expected any) (bool, error) {
	if expectsActualCollection(op) {
		actualSlice, err := sanitizeActualCollection(dt, actual)
		if err != nil {
			return false, err
		}
		return evaluateCollectionOperator(dt, op, actualSlice, expected)
	}

	actualValue, err := sanitizeActualValue(dt, actual)
	if err != nil {
		return false, err
	}
	return evaluateScalarOperator(dt, op, actualValue, expected)
}

func evaluateScalarOperator(dt DataType, op OperatorType, actual any, expected any) (bool, error) {
	switch op {
	case OperatorEqual:
		return equals(dt, actual, expected)
	case OperatorNotEqual:
		match, err := equals(dt, actual, expected)
		return !match, err
	case OperatorGreater, OperatorGreaterOrEqual, OperatorLess, OperatorLessOrEqual:
		return compare(dt, op, actual, expected)
	case OperatorIn:
		expectedSlice, ok := expected.([]any)
		if !ok {
			return false, fmt.Errorf("operator IN expects slice value, got %T", expected)
		}
		return containsValue(dt, expectedSlice, actual)
	case OperatorNotIn:
		expectedSlice, ok := expected.([]any)
		if !ok {
			return false, fmt.Errorf("operator NOT_IN expects slice value, got %T", expected)
		}
		result, err := containsValue(dt, expectedSlice, actual)
		return !result, err
	case OperatorMatchesRegex:
		if actual == nil {
			return false, nil
		}
		re, ok := expected.(*regexp.Regexp)
		if !ok {
			return false, fmt.Errorf("operator MATCHES_REGEX expects compiled regexp, got %T", expected)
		}
		value, ok := actual.(string)
		if !ok {
			return false, fmt.Errorf("operator MATCHES_REGEX expects string actual, got %T", actual)
		}
		return re.MatchString(value), nil
	case OperatorIsNull:
		return actual == nil, nil
	case OperatorIsNotNull:
		return actual != nil, nil
	default:
		return false, fmt.Errorf("%w: %s", ErrUnsupportedOperator, op)
	}
}

func evaluateCollectionOperator(dt DataType, op OperatorType, actual []any, expected any) (bool, error) {
	switch op {
	case OperatorAnyContained:
		expectedSlice, ok := expected.([]any)
		if !ok {
			return false, fmt.Errorf("operator ANY_CONTAINED_IN expects slice value, got %T", expected)
		}
		for _, v := range actual {
			match, err := containsValue(dt, expectedSlice, v)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	case OperatorNotAnyContained:
		match, err := evaluateCollectionOperator(dt, OperatorAnyContained, actual, expected)
		return !match, err
	case OperatorAllContained:
		expectedSlice, ok := expected.([]any)
		if !ok {
			return false, fmt.Errorf("operator ALL_CONTAINED_IN expects slice value, got %T", expected)
		}
		for _, v := range actual {
			match, err := containsValue(dt, expectedSlice, v)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	case OperatorNotAllContained:
		match, err := evaluateCollectionOperator(dt, OperatorAllContained, actual, expected)
		return !match, err
	case OperatorContainsAll:
		expectedSlice, ok := expected.([]any)
		if !ok {
			return false, fmt.Errorf("operator CONTAINS_ALL expects slice value, got %T", expected)
		}
		for _, v := range expectedSlice {
			match, err := containsValue(dt, actual, v)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	case OperatorNotContainsAll:
		match, err := evaluateCollectionOperator(dt, OperatorContainsAll, actual, expected)
		return !match, err
	case OperatorAllEqual:
		if expected == nil {
			return false, fmt.Errorf("operator ALL_EQUAL expects a scalar value")
		}
		for _, v := range actual {
			match, err := equals(dt, v, expected)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return len(actual) > 0, nil
	default:
		return false, fmt.Errorf("%w: %s", ErrUnsupportedOperator, op)
	}
}

func equals(dt DataType, left any, right any) (bool, error) {
	if left == nil || right == nil {
		return left == nil && right == nil, nil
	}
	switch dt {
	case DataTypeString:
		lhs, lok := left.(string)
		rhs, rok := right.(string)
		if !lok || !rok {
			return false, fmt.Errorf("values not strings: %T vs %T", left, right)
		}
		return lhs == rhs, nil
	case DataTypeInteger:
		lhs, lok := left.(int64)
		rhs, rok := right.(int64)
		if !lok || !rok {
			return false, fmt.Errorf("values not int64: %T vs %T", left, right)
		}
		return lhs == rhs, nil
	case DataTypeDecimal:
		lbd, lok := left.(*big.Float)
		rbd, rok := right.(*big.Float)
		if !lok || !rok {
			return false, fmt.Errorf("values not decimal: %T vs %T", left, right)
		}
		return lbd.Cmp(rbd) == 0, nil
	case DataTypeBoolean:
		lhs, lok := left.(bool)
		rhs, rok := right.(bool)
		if !lok || !rok {
			return false, fmt.Errorf("values not bool: %T vs %T", left, right)
		}
		return lhs == rhs, nil
	case DataTypeDate, DataTypeDateTime:
		lhs, lok := left.(time.Time)
		rhs, rok := right.(time.Time)
		if !lok || !rok {
			return false, fmt.Errorf("values not time.Time: %T vs %T", left, right)
		}
		return lhs.Equal(rhs), nil
	case DataTypeListString:
		return equalsList(DataTypeString, left, right)
	case DataTypeListInteger:
		return equalsList(DataTypeInteger, left, right)
	default:
		return false, fmt.Errorf("unsupported data type %s", dt)
	}
}

func compare(dt DataType, op OperatorType, left any, right any) (bool, error) {
	if left == nil || right == nil {
		return false, nil
	}
	var l, r float64
	switch dt {
	case DataTypeInteger:
		lv, lok := left.(int64)
		rv, rok := right.(int64)
		if !lok || !rok {
			return false, fmt.Errorf("values not int64: %T vs %T", left, right)
		}
		l = float64(lv)
		r = float64(rv)
	case DataTypeDecimal:
		lbd, lok := left.(*big.Float)
		rbd, rok := right.(*big.Float)
		if !lok || !rok {
			return false, fmt.Errorf("values not decimal: %T vs %T", left, right)
		}
		switch op {
		case OperatorGreater:
			return lbd.Cmp(rbd) > 0, nil
		case OperatorGreaterOrEqual:
			return lbd.Cmp(rbd) >= 0, nil
		case OperatorLess:
			return lbd.Cmp(rbd) < 0, nil
		case OperatorLessOrEqual:
			return lbd.Cmp(rbd) <= 0, nil
		default:
			return false, fmt.Errorf("%w: %s", ErrUnsupportedOperator, op)
		}
	case DataTypeDate, DataTypeDateTime:
		lTime, lok := left.(time.Time)
		rTime, rok := right.(time.Time)
		if !lok || !rok {
			return false, fmt.Errorf("values not time.Time: %T vs %T", left, right)
		}
		switch op {
		case OperatorGreater:
			return lTime.After(rTime), nil
		case OperatorGreaterOrEqual:
			return !lTime.Before(rTime), nil
		case OperatorLess:
			return lTime.Before(rTime), nil
		case OperatorLessOrEqual:
			return !lTime.After(rTime), nil
		default:
			return false, fmt.Errorf("%w: %s", ErrUnsupportedOperator, op)
		}
	default:
		return false, fmt.Errorf("operator %s only supported for numeric columns", op)
	}

	switch op {
	case OperatorGreater:
		return l > r, nil
	case OperatorGreaterOrEqual:
		return l >= r, nil
	case OperatorLess:
		return l < r, nil
	case OperatorLessOrEqual:
		return l <= r, nil
	default:
		return false, fmt.Errorf("%w: %s", ErrUnsupportedOperator, op)
	}
}

func containsValue(dt DataType, haystack []any, needle any) (bool, error) {
	elemType := elementDataType(dt)
	for _, candidate := range haystack {
		match, err := equals(elemType, candidate, needle)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func equalsList(elemType DataType, left any, right any) (bool, error) {
	lhs, lok := left.([]any)
	rhs, rok := right.([]any)
	if !lok || !rok {
		return false, fmt.Errorf("values not list: %T vs %T", left, right)
	}
	if len(lhs) != len(rhs) {
		return false, nil
	}
	for i := range lhs {
		match, err := equals(elemType, lhs[i], rhs[i])
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}
