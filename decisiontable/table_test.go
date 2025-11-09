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

import "testing"

func TestDecisionTableReturnsAllMatches(t *testing.T) {
	dt := buildSampleTable(t)

	input := map[string]any{
		"age":      32,
		"country":  "US",
		"segments": []string{"vip", "beta"},
	}

	rows, err := dt.Evaluate(input, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	if rows[0].RowNumber != 1 || rows[0].Values["tier"] != "standard" {
		t.Fatalf("first row mismatch: %#v", rows[0])
	}
	if rows[1].RowNumber != 2 || rows[1].Values["tier"] != "premium" {
		t.Fatalf("second row mismatch: %#v", rows[1])
	}
	if rows[2].RowNumber != 3 || rows[2].Values["tier"] != "vip-only" {
		t.Fatalf("third row mismatch: %#v", rows[2])
	}
}

func TestDecisionTableFirstMatch(t *testing.T) {
	dt := buildSampleTable(t, WithMatchPolicy(MatchPolicyFirst))

	rows, err := dt.Evaluate(map[string]any{
		"age":      40,
		"country":  "US",
		"segments": []string{"vip"},
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly one row, got %d", len(rows))
	}
	if rows[0].RowNumber != 1 {
		t.Fatalf("expected first row, got %d", rows[0].RowNumber)
	}
}

func TestDecisionTableDefaultReturn(t *testing.T) {
	dt := buildSampleTable(t, WithNoMatchPolicy(NoMatchPolicyReturnDefault))

	err := dt.SetDefaultRow(Row{
		RuleID: "default-row",
		ReturnCells: []ReturnCell{
			{Column: "tier", Value: "minor"},
			{Column: "discount", Value: 0.0},
		},
	})
	if err != nil {
		t.Fatalf("failed to set default row: %v", err)
	}

	rows, err := dt.Evaluate(map[string]any{
		"age":     12,
		"country": "US",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected default row, got %d rows", len(rows))
	}
	if rows[0].RowNumber != dt.RowCount()+1 {
		t.Fatalf("expected default row number %d, got %d", dt.RowCount()+1, rows[0].RowNumber)
	}
	if rows[0].Values["tier"] != "minor" {
		t.Fatalf("expected tier minor, got %v", rows[0].Values["tier"])
	}
}

func TestDecisionTableFallbackDefaultReturn(t *testing.T) {
	dt := buildSampleTable(t, WithNoMatchPolicy(NoMatchPolicyReturnDefault))

	defaultRow := map[string]any{"tier": "fallback", "discount": 0.0}
	rows, err := dt.Evaluate(map[string]any{
		"age":     5,
		"country": "US",
	}, defaultRow)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected fallback row, got %d rows", len(rows))
	}
	if rows[0].Values["tier"] != "fallback" {
		t.Fatalf("expected fallback tier, got %v", rows[0].Values["tier"])
	}
	if rows[0].RowNumber != 0 {
		t.Fatalf("expected fallback row number 0, got %d", rows[0].RowNumber)
	}
}

func TestDecisionTableDecimal(t *testing.T) {
	evalCols := []Column{
		{Name: "amount", Type: ColumnTypeCondition, DataType: DataTypeDecimal},
	}
	retCols := []Column{
		{Name: "tier", Type: ColumnTypeConclusion, DataType: DataTypeString},
	}

	dt, err := NewDecisionTable("payments", evalCols, retCols)
	if err != nil {
		t.Fatalf("failed to build table: %v", err)
	}

	err = dt.AddRow(Row{
		RuleID: "bd-1",
		EvalCells: []EvalCell{
			{Column: "amount", Operator: OperatorGreaterOrEqual, Value: "99.999999999999999999"},
		},
		ReturnCells: []ReturnCell{
			{Column: "tier", Value: "preferred"},
		},
	})
	if err != nil {
		t.Fatalf("failed to add row: %v", err)
	}

	rows, err := dt.Evaluate(map[string]any{
		"amount": "100.000000000000000001",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected big decimal match, got %d rows", len(rows))
	}
	if rows[0].Values["tier"] != "preferred" {
		t.Fatalf("expected preferred tier, got %v", rows[0].Values["tier"])
	}
}

func buildSampleTable(t *testing.T, opts ...Option) *DecisionTable {
	t.Helper()
	evalCols := []Column{
		{Name: "age", Type: ColumnTypeCondition, DataType: DataTypeInteger},
		{Name: "country", Type: ColumnTypeCondition, DataType: DataTypeString},
		{Name: "segments", Type: ColumnTypeCondition, DataType: DataTypeListString},
	}
	retCols := []Column{
		{Name: "tier", Type: ColumnTypeConclusion, DataType: DataTypeString},
		{Name: "discount", Type: ColumnTypeConclusion, DataType: DataTypeDecimal},
	}

	dt, err := NewDecisionTable("eligibility", evalCols, retCols, opts...)
	if err != nil {
		t.Fatalf("failed to build table: %v", err)
	}

	rows := []Row{
		{
			RuleID: "eligibility-standard",
			EvalCells: []EvalCell{
				{Column: "age", Operator: OperatorGreaterOrEqual, Value: 18},
				{Column: "country", Operator: OperatorIn, Value: []string{"US", "CA"}},
			},
			ReturnCells: []ReturnCell{
				{Column: "tier", Value: "standard"},
				{Column: "discount", Value: 0.05},
			},
		},
		{
			RuleID: "eligibility-premium",
			EvalCells: []EvalCell{
				{Column: "age", Operator: OperatorGreaterOrEqual, Value: 30},
				{Column: "country", Operator: OperatorEqual, Value: "US"},
			},
			ReturnCells: []ReturnCell{
				{Column: "tier", Value: "premium"},
				{Column: "discount", Value: 0.15},
			},
		},
		{
			RuleID: "eligibility-vip-segment",
			EvalCells: []EvalCell{
				{Column: "segments", Operator: OperatorAnyContained, Value: []string{"vip"}},
			},
			ReturnCells: []ReturnCell{
				{Column: "tier", Value: "vip-only"},
				{Column: "discount", Value: 0.2},
			},
		},
	}

	for _, row := range rows {
		if err := dt.AddRow(row); err != nil {
			t.Fatalf("failed to add row %s: %v", row.RuleID, err)
		}
	}
	return dt
}
