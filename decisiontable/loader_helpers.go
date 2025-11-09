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
	"strings"
)

func normalizeKeyword(s string) string {
	upper := strings.ToUpper(strings.TrimSpace(s))
	upper = strings.ReplaceAll(upper, " ", "_")
	return upper
}

func parseMatchPolicyString(s string) (MatchPolicy, error) {
	switch normalizeKeyword(s) {
	case "FIRST":
		return MatchPolicyFirst, nil
	case "ALL":
		return MatchPolicyAll, nil
	case "UNIQUE":
		return MatchPolicyUnique, nil
	default:
		return MatchPolicyAll, fmt.Errorf("unknown match policy %q", s)
	}
}

func parseNoMatchPolicyString(s string) (NoMatchPolicy, error) {
	switch normalizeKeyword(s) {
	case "RETURN_DEFAULT":
		return NoMatchPolicyReturnDefault, nil
	case "THROW_ERROR":
		return NoMatchPolicyThrowError, nil
	default:
		return NoMatchPolicyThrowError, fmt.Errorf("unknown no-match policy %q", s)
	}
}

func parseRowValidationPolicyString(s string) (RowValidationPolicy, error) {
	switch normalizeKeyword(s) {
	case "", "DEFAULT":
		return RowValidationStrict, nil
	case "STRICT":
		return RowValidationStrict, nil
	case "LENIENT":
		return RowValidationLenient, nil
	default:
		return RowValidationStrict, fmt.Errorf("unknown row validation policy %q", s)
	}
}

func parseColumnTypeString(s string) (ColumnType, error) {
	switch normalizeKeyword(s) {
	case "CONDITION":
		return ColumnTypeCondition, nil
	case "CONCLUSION":
		return ColumnTypeConclusion, nil
	case "METADATA":
		return ColumnTypeMetadata, nil
	default:
		return "", fmt.Errorf("unknown column type %q", s)
	}
}

func parseDataTypeString(s string) (DataType, error) {
	switch normalizeKeyword(s) {
	case "STRING":
		return DataTypeString, nil
	case "INTEGER":
		return DataTypeInteger, nil
	case "BOOLEAN":
		return DataTypeBoolean, nil
	case "DECIMAL":
		return DataTypeDecimal, nil
	case "DATE":
		return DataTypeDate, nil
	case "DATETIME":
		return DataTypeDateTime, nil
	case "LIST_STRING":
		return DataTypeListString, nil
	case "LIST_INTEGER":
		return DataTypeListInteger, nil
	default:
		return "", fmt.Errorf("unknown data type %q", s)
	}
}

func parseJSONOperatorToken(token string) (OperatorType, error) {
	switch normalizeKeyword(token) {
	case "EQUAL":
		return OperatorEqual, nil
	case "NOTEQUAL", "NOT_EQUAL":
		return OperatorNotEqual, nil
	case "GREATERTHAN", "GREATER_THAN":
		return OperatorGreater, nil
	case "GREATERTHANOREQUAL", "GREATER_THAN_OR_EQUAL":
		return OperatorGreaterOrEqual, nil
	case "LESSTHAN", "LESS_THAN":
		return OperatorLess, nil
	case "LESSTHANOREQUAL", "LESS_THAN_OR_EQUAL":
		return OperatorLessOrEqual, nil
	case "IN":
		return OperatorIn, nil
	case "NOTIN", "NOT_IN":
		return OperatorNotIn, nil
	case "MATCHESREGEX", "MATCHES_REGEX":
		return OperatorMatchesRegex, nil
	case "ISNULL", "IS_NULL":
		return OperatorIsNull, nil
	case "ISNOTNULL", "IS_NOT_NULL":
		return OperatorIsNotNull, nil
	default:
		return "", fmt.Errorf("unknown operator %q", token)
	}
}

func parseOperatorToken(token string) (OperatorType, error) {
	tok := strings.TrimSpace(token)
	switch tok {
	case ">=":
		return OperatorGreaterOrEqual, nil
	case ">":
		return OperatorGreater, nil
	case "=":
		return OperatorEqual, nil
	case "<":
		return OperatorLess, nil
	case "<=":
		return OperatorLessOrEqual, nil
	case "<>", "!=":
		return OperatorNotEqual, nil
	}

	switch normalizeKeyword(tok) {
	case "GT_EQ":
		return OperatorGreaterOrEqual, nil
	case "GT":
		return OperatorGreater, nil
	case "EQ":
		return OperatorEqual, nil
	case "LT":
		return OperatorLess, nil
	case "LT_EQ":
		return OperatorLessOrEqual, nil
	case "NOT_EQ":
		return OperatorNotEqual, nil
	case "IN":
		return OperatorIn, nil
	case "NOT_IN":
		return OperatorNotIn, nil
	case "ANY_CONTAINED_IN":
		return OperatorAnyContained, nil
	case "NOT_ANY_CONTAINED_IN":
		return OperatorNotAnyContained, nil
	case "ALL_CONTAINED_IN":
		return OperatorAllContained, nil
	case "NOT_ALL_CONTAINED_IN":
		return OperatorNotAllContained, nil
	case "CONTAINS_ALL":
		return OperatorContainsAll, nil
	case "NOT_CONTAINS_ALL":
		return OperatorNotContainsAll, nil
	case "ALL_EQUAL":
		return OperatorAllEqual, nil
	default:
		return "", fmt.Errorf("unknown operator %q", token)
	}
}

func splitAndDedupeList(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	seen := make(map[string]struct{}, len(parts))
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no values provided")
	}
	return result, nil
}
