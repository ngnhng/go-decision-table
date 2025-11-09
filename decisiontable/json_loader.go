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
	"fmt"
	"os"
	"strings"
)

// LoadJSONFile loads a decision table from a JSON file that follows the new DSL spec.
func LoadJSONFile(path string) (*DecisionTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read json file %s: %w", path, err)
	}
	return LoadJSON(data, path)
}

// LoadJSON loads a decision table from raw JSON bytes.
func LoadJSON(data []byte, name string) (*DecisionTable, error) {
	var doc jsonDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	if doc.DecisionTable == nil {
		return nil, fmt.Errorf("json does not contain a decisionTable object")
	}
	return buildDecisionTable(*doc.DecisionTable, name)
}

type jsonDocument struct {
	DecisionTable *jsonDecisionTableSpec `json:"decisionTable"`
}

type jsonDecisionTableSpec struct {
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Policies    jsonPoliciesSpec     `json:"policies"`
	Columns     []jsonColumnSpec     `json:"columns"`
	Rules       []jsonRuleSpec       `json:"rules"`
	DefaultRule *jsonDefaultRuleSpec `json:"defaultRule"`
}

type jsonPoliciesSpec struct {
	MatchPolicy   string `json:"matchPolicy"`
	NoMatchPolicy string `json:"noMatchPolicy"`
}

type jsonColumnSpec struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Type     string `json:"type"`
	DataType string `json:"dataType"`
}

type jsonRuleSpec struct {
	ID          string               `json:"id"`
	Description string               `json:"description"`
	When        []*jsonConditionCell `json:"when"`
	Then        []any                `json:"then"`
}

type jsonConditionCell struct {
	Operator string `json:"operator"`
	Value    any    `json:"value"`
}

type jsonDefaultRuleSpec struct {
	Description string `json:"description"`
	Then        []any  `json:"then"`
}

func buildDecisionTable(spec jsonDecisionTableSpec, sourceName string) (*DecisionTable, error) {
	if len(spec.Columns) == 0 {
		return nil, fmt.Errorf("decision table requires at least one column")
	}
	if len(spec.Rules) == 0 {
		return nil, fmt.Errorf("decision table requires at least one rule")
	}
	if spec.Policies.MatchPolicy == "" {
		return nil, fmt.Errorf("policies.matchPolicy is required")
	}
	if spec.Policies.NoMatchPolicy == "" {
		return nil, fmt.Errorf("policies.noMatchPolicy is required")
	}

	mp, err := parseMatchPolicyString(spec.Policies.MatchPolicy)
	if err != nil {
		return nil, err
	}
	nmp, err := parseNoMatchPolicyString(spec.Policies.NoMatchPolicy)
	if err != nil {
		return nil, err
	}

	conditionCols, outputCols, err := convertColumns(spec.Columns)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(spec.Name)
	if name == "" {
		name = sourceName
	}

	dt, err := NewDecisionTable(name, conditionCols, outputCols, WithMatchPolicy(mp), WithNoMatchPolicy(nmp))
	if err != nil {
		return nil, err
	}

	ruleIDs := make(map[string]struct{})
	for idx, rule := range spec.Rules {
		row, err := convertRule(rule, conditionCols, outputCols, idx+1, ruleIDs)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", idx+1, err)
		}
		if err := dt.AddRow(row); err != nil {
			return nil, fmt.Errorf("rule %d: %w", idx+1, err)
		}
	}

	if spec.DefaultRule != nil {
		defaultRow, err := convertDefaultRule(*spec.DefaultRule, outputCols, len(spec.Rules)+1, ruleIDs)
		if err != nil {
			return nil, err
		}
		if err := dt.SetDefaultRow(defaultRow); err != nil {
			return nil, err
		}
	} else if nmp == NoMatchPolicyReturnDefault {
		return nil, fmt.Errorf("defaultRule section required for RETURN_DEFAULT policy")
	}

	return dt, nil
}

func convertColumns(cols []jsonColumnSpec) ([]Column, []Column, error) {
	conditions := make([]Column, 0, len(cols))
	outputs := make([]Column, 0, len(cols))
	seen := make(map[string]struct{}, len(cols))
	for idx, col := range cols {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			return nil, nil, fmt.Errorf("column %d name cannot be empty", idx+1)
		}
		if _, exists := seen[name]; exists {
			return nil, nil, fmt.Errorf("duplicate column name %q", name)
		}
		colType, err := parseColumnTypeString(col.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("column %s: %w", name, err)
		}
		dataType, err := parseDataTypeString(col.DataType)
		if err != nil {
			return nil, nil, fmt.Errorf("column %s: %w", name, err)
		}
		column := Column{Name: name, Type: colType, DataType: dataType}
		switch colType {
		case ColumnTypeCondition:
			conditions = append(conditions, column)
		case ColumnTypeConclusion, ColumnTypeMetadata:
			outputs = append(outputs, column)
		}
		seen[name] = struct{}{}
	}
	if len(conditions) == 0 {
		return nil, nil, fmt.Errorf("at least one CONDITION column is required")
	}
	if len(outputs) == 0 {
		return nil, nil, fmt.Errorf("at least one CONCLUSION or METADATA column is required")
	}
	return conditions, outputs, nil
}

func convertRule(rule jsonRuleSpec, conditionCols, outputCols []Column, rowNumber int, ruleIDs map[string]struct{}) (Row, error) {
	if len(rule.When) != len(conditionCols) {
		return Row{}, fmt.Errorf("expected %d when cells, got %d", len(conditionCols), len(rule.When))
	}
	if len(rule.Then) != len(outputCols) {
		return Row{}, fmt.Errorf("expected %d then values, got %d", len(outputCols), len(rule.Then))
	}
	row := Row{
		Number:   rowNumber,
		RuleID:   strings.TrimSpace(rule.ID),
		Comments: rule.Description,
	}
	for idx, column := range conditionCols {
		cell := rule.When[idx]
		if cell == nil {
			continue
		}
		operator := strings.TrimSpace(cell.Operator)
		if operator == "" {
			if cell.Value != nil {
				return Row{}, fmt.Errorf("column %s: operator is required when value is provided", column.Name)
			}
			continue
		}
		op, err := parseJSONOperatorToken(operator)
		if err != nil {
			return Row{}, fmt.Errorf("column %s: %w", column.Name, err)
		}
		if cell.Value == nil && op != OperatorIsNull && op != OperatorIsNotNull {
			return Row{}, fmt.Errorf("column %s: operator %s requires a value", column.Name, operator)
		}
		row.EvalCells = append(row.EvalCells, EvalCell{
			Column:   column.Name,
			Operator: op,
			Value:    cell.Value,
		})
	}
	for idx, column := range outputCols {
		row.ReturnCells = append(row.ReturnCells, ReturnCell{
			Column: column.Name,
			Value:  rule.Then[idx],
		})
	}
	if err := ensureUniqueRuleID(&row, rowNumber, ruleIDs); err != nil {
		return Row{}, err
	}
	return row, nil
}

func convertDefaultRule(rule jsonDefaultRuleSpec, outputCols []Column, rowNumber int, ruleIDs map[string]struct{}) (Row, error) {
	if len(rule.Then) != len(outputCols) {
		return Row{}, fmt.Errorf("defaultRule: expected %d values, got %d", len(outputCols), len(rule.Then))
	}
	row := Row{
		Number:   rowNumber,
		Comments: rule.Description,
	}
	for idx, column := range outputCols {
		row.ReturnCells = append(row.ReturnCells, ReturnCell{
			Column: column.Name,
			Value:  rule.Then[idx],
		})
	}
	if err := ensureUniqueRuleID(&row, rowNumber, ruleIDs); err != nil {
		return Row{}, err
	}
	return row, nil
}

func ensureUniqueRuleID(row *Row, rowNumber int, ruleIDs map[string]struct{}) error {
	id := strings.TrimSpace(row.RuleID)
	if id == "" {
		id = fmt.Sprintf("%d", rowNumber)
	}
	if _, exists := ruleIDs[id]; exists {
		return fmt.Errorf("duplicate rule id %q", id)
	}
	ruleIDs[id] = struct{}{}
	row.RuleID = id
	return nil
}
