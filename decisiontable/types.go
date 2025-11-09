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

// ColumnType describes how a column participates in the decision table.
type ColumnType string

const (
	ColumnTypeCondition  ColumnType = "CONDITION"
	ColumnTypeConclusion ColumnType = "CONCLUSION"
	ColumnTypeMetadata   ColumnType = "METADATA"
)

type DataType string

const (
	DataTypeString      DataType = "STRING"
	DataTypeInteger     DataType = "INTEGER"
	DataTypeBoolean     DataType = "BOOLEAN"
	DataTypeDecimal     DataType = "DECIMAL"
	DataTypeDate        DataType = "DATE"
	DataTypeDateTime    DataType = "DATETIME"
	DataTypeListString  DataType = "LIST_STRING"
	DataTypeListInteger DataType = "LIST_INTEGER"
)

// OperatorType controls how an evaluation cell compares its actual value with the expected value.
type OperatorType string

const (
	OperatorGreaterOrEqual  OperatorType = "GT_EQ"
	OperatorGreater         OperatorType = "GT"
	OperatorEqual           OperatorType = "EQ"
	OperatorLess            OperatorType = "LT"
	OperatorLessOrEqual     OperatorType = "LT_EQ"
	OperatorNotEqual        OperatorType = "NOT_EQ"
	OperatorIn              OperatorType = "IN"
	OperatorNotIn           OperatorType = "NOT_IN"
	OperatorAnyContained    OperatorType = "ANY_CONTAINED_IN"
	OperatorNotAnyContained OperatorType = "NOT_ANY_CONTAINED_IN"
	OperatorAllContained    OperatorType = "ALL_CONTAINED_IN"
	OperatorNotAllContained OperatorType = "NOT_ALL_CONTAINED_IN"
	OperatorContainsAll     OperatorType = "CONTAINS_ALL"
	OperatorNotContainsAll  OperatorType = "NOT_CONTAINS_ALL"
	OperatorAllEqual        OperatorType = "ALL_EQUAL"
	OperatorMatchesRegex    OperatorType = "MATCHES_REGEX"
	OperatorIsNull          OperatorType = "IS_NULL"
	OperatorIsNotNull       OperatorType = "IS_NOT_NULL"
)

// MatchPolicy describes how many rows should be returned after evaluation.
type MatchPolicy int

const (
	MatchPolicyFirst MatchPolicy = iota
	MatchPolicyAll
	MatchPolicyUnique
)

// NoMatchPolicy dictates what evaluate should return when no rows match.
type NoMatchPolicy int

const (
	NoMatchPolicyReturnDefault NoMatchPolicy = iota
	NoMatchPolicyThrowError
)

type RowValidationPolicy int

const (
	RowValidationStrict RowValidationPolicy = iota
	RowValidationLenient
)

// Column defines metadata for a decision table column.
type Column struct {
	Name     string
	Type     ColumnType
	DataType DataType
}

// EvalCell configures a single evaluation condition inside a row.
type EvalCell struct {
	Column   string
	Operator OperatorType
	Value    any
	dataType DataType
}

// ReturnCell stores the payload that will be produced when a row matches.
type ReturnCell struct {
	Column   string
	Value    any
	dataType DataType
}

// Row models a single decision table row.
type Row struct {
	EvalCells   []EvalCell
	ReturnCells []ReturnCell
	RuleID      string
	Comments    string
	Number      int
}

// MatchedRow represents the outcome for a matched rule.
type MatchedRow struct {
	Values    map[string]any
	RuleID    string
	Comments  string
	RowNumber int
}

// Option allows configuring a DecisionTable during construction.
type Option func(*DecisionTable)

// WithMatchPolicy overrides the default match policy (MatchPolicyAll).
func WithMatchPolicy(mp MatchPolicy) Option {
	return func(dt *DecisionTable) {
		dt.matchPolicy = mp
	}
}

// WithNoMatchPolicy overrides the default no-match policy (NoMatchPolicyThrowError).
func WithNoMatchPolicy(nmp NoMatchPolicy) Option {
	return func(dt *DecisionTable) {
		dt.noMatchPolicy = nmp
	}
}

// WithRowValidationPolicy explicitly sets the validation policy to use when rows are added.
func WithRowValidationPolicy(rvp RowValidationPolicy) Option {
	return func(dt *DecisionTable) {
		dt.rowValidation = rvp
	}
}

func (c Column) validate() error {
	if c.Name == "" {
		return fmt.Errorf("column name must be provided")
	}
	switch c.Type {
	case ColumnTypeCondition, ColumnTypeConclusion, ColumnTypeMetadata:
		// supported
	default:
		return fmt.Errorf("column %s has unsupported type %s", c.Name, c.Type)
	}
	switch c.DataType {
	case DataTypeString,
		DataTypeInteger,
		DataTypeBoolean,
		DataTypeDecimal,
		DataTypeDate,
		DataTypeDateTime,
		DataTypeListString,
		DataTypeListInteger:
		return nil
	default:
		return fmt.Errorf("column %s has unsupported data type %s", c.Name, c.DataType)
	}
}
