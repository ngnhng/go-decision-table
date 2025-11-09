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

// DecisionTable is the in-memory representation of a decision table ready for evaluation.
type DecisionTable struct {
	Name string

	conditionColumns map[string]Column
	outputColumns    map[string]Column
	rows             []Row
	defaultRow       *Row

	matchPolicy   MatchPolicy
	noMatchPolicy NoMatchPolicy
	rowValidation RowValidationPolicy
}

// NewDecisionTable constructs a decision table given separate evaluation and return columns.
func NewDecisionTable(name string, conditionCols, outputCols []Column, opts ...Option) (*DecisionTable, error) {
	if name == "" {
		return nil, fmt.Errorf("table name must not be empty")
	}

	if len(conditionCols) == 0 {
		return nil, fmt.Errorf("table must define at least one condition column")
	}
	if len(outputCols) == 0 {
		return nil, fmt.Errorf("table must define at least one output column")
	}

	conditionMap, err := buildColumnMap(conditionCols, ColumnTypeCondition)
	if err != nil {
		return nil, err
	}
	outputMap, err := buildColumnMap(outputCols, ColumnTypeConclusion, ColumnTypeMetadata)
	if err != nil {
		return nil, err
	}

	dt := &DecisionTable{
		Name:             name,
		conditionColumns: conditionMap,
		outputColumns:    outputMap,
		matchPolicy:      MatchPolicyAll,
		noMatchPolicy:    NoMatchPolicyThrowError,
		rowValidation:    RowValidationStrict,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(dt)
		}
	}
	return dt, nil
}

// AddRow registers a decision table row. The incoming row is copied and sanitized.
func (dt *DecisionTable) AddRow(row Row) error {
	prepared, err := dt.prepareRow(row, dt.rowValidation == RowValidationStrict, dt.rowValidation == RowValidationStrict)
	if err != nil {
		return err
	}
	if prepared.Number <= 0 {
		prepared.Number = len(dt.rows) + 1
	}
	dt.rows = append(dt.rows, prepared)
	return nil
}

// SetDefaultRow registers a default row that will be returned automatically when no rules match.
func (dt *DecisionTable) SetDefaultRow(row Row) error {
	if dt.noMatchPolicy != NoMatchPolicyReturnDefault && dt.noMatchPolicy != NoMatchPolicyThrowError {
		return fmt.Errorf("default rows are only valid when using RETURN_DEFAULT or THROW_ERROR no-match policy")
	}
	if len(row.EvalCells) > 0 {
		return fmt.Errorf("default rows cannot contain evaluation cells")
	}
	prepared, err := dt.prepareRow(row, false, true)
	if err != nil {
		return err
	}
	if prepared.Number <= 0 {
		prepared.Number = len(dt.rows) + 1
	}
	copied := prepared
	dt.defaultRow = &copied
	return nil
}

// Evaluate processes the supplied input map and returns the rows that match the configured policy.
// When there are no matches and the table is configured with RETURN_DEFAULT, the supplied defaultReturn map is returned.
func (dt *DecisionTable) Evaluate(input map[string]any, defaultReturn map[string]any) ([]MatchedRow, error) {
	var matches []MatchedRow
	for _, row := range dt.rows {
		match, err := row.matches(input)
		if err != nil {
			return nil, err
		}
		if match {
			matches = append(matches, MatchedRow{
				Values:    row.materializeReturnValues(),
				RuleID:    row.RuleID,
				Comments:  row.Comments,
				RowNumber: row.Number,
			})
			if dt.matchPolicy == MatchPolicyFirst {
				break
			}
			if dt.matchPolicy == MatchPolicyUnique && len(matches) > 1 {
				return nil, fmt.Errorf("match policy UNIQUE expected exactly one match, found at least %d", len(matches))
			}
		}
	}

	if len(matches) == 0 {
		switch dt.noMatchPolicy {
		case NoMatchPolicyReturnDefault:
			switch {
			case dt.defaultRow != nil:
				matches = append(matches, MatchedRow{
					Values:    dt.defaultRow.materializeReturnValues(),
					RuleID:    dt.defaultRow.RuleID,
					Comments:  dt.defaultRow.Comments,
					RowNumber: dt.defaultRow.Number,
				})
			case defaultReturn != nil:
				matches = append(matches, MatchedRow{
					Values: cloneMap(defaultReturn),
				})
			}
		case NoMatchPolicyThrowError:
			if dt.defaultRow != nil {
				matches = append(matches, MatchedRow{
					Values:    dt.defaultRow.materializeReturnValues(),
					RuleID:    dt.defaultRow.RuleID,
					Comments:  dt.defaultRow.Comments,
					RowNumber: dt.defaultRow.Number,
				})
			} else {
				return nil, fmt.Errorf("no rules matched and no default rule configured")
			}
		}
	} else if dt.matchPolicy == MatchPolicyUnique {
		// exactly one match already enforced during iteration
		return matches, nil
	}

	return matches, nil
}

// Rows returns a shallow copy of the registered rows so callers cannot mutate the internal slice.
func (dt *DecisionTable) Rows() []Row {
	if dt == nil {
		return nil
	}
	out := make([]Row, len(dt.rows))
	copy(out, dt.rows)
	return out
}

// RowCount exposes the number of rows currently stored.
func (dt *DecisionTable) RowCount() int {
	if dt == nil {
		return 0
	}
	return len(dt.rows)
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = cloneArbitraryValue(v)
	}
	return dst
}

func buildColumnMap(cols []Column, allowedTypes ...ColumnType) (map[string]Column, error) {
	result := make(map[string]Column, len(cols))
	allowed := make(map[ColumnType]struct{}, len(allowedTypes))
	for _, t := range allowedTypes {
		allowed[t] = struct{}{}
	}
	for _, c := range cols {
		if err := c.validate(); err != nil {
			return nil, err
		}
		if _, ok := allowed[c.Type]; !ok {
			return nil, fmt.Errorf("column %s must be one of %v", c.Name, allowedTypes)
		}
		if _, exists := result[c.Name]; exists {
			return nil, fmt.Errorf("duplicate column %s", c.Name)
		}
		result[c.Name] = c
	}
	return result, nil
}

func (dt *DecisionTable) prepareRow(row Row, requireEval bool, requireReturn bool) (Row, error) {
	if dt == nil {
		return Row{}, fmt.Errorf("decision table is nil")
	}
	if requireEval && len(row.EvalCells) == 0 {
		return Row{}, fmt.Errorf("row must contain at least one evaluation cell")
	}
	if requireReturn && len(row.ReturnCells) == 0 {
		return Row{}, fmt.Errorf("row must contain at least one return cell")
	}

	prepared := Row{
		EvalCells:   make([]EvalCell, len(row.EvalCells)),
		ReturnCells: make([]ReturnCell, len(row.ReturnCells)),
		RuleID:      row.RuleID,
		Comments:    row.Comments,
		Number:      row.Number,
	}

	for i, cell := range row.EvalCells {
		col, ok := dt.conditionColumns[cell.Column]
		if !ok {
			return Row{}, fmt.Errorf("%w %q", ErrUnknownColumn, cell.Column)
		}
		if cell.Operator == "" {
			return Row{}, fmt.Errorf("column %s missing operator", col.Name)
		}
		value, err := sanitizeExpectedValue(col.DataType, cell.Operator, cell.Value)
		if err != nil {
			return Row{}, fmt.Errorf("column %s: %w", col.Name, err)
		}
		prepared.EvalCells[i] = EvalCell{
			Column:   col.Name,
			Operator: cell.Operator,
			Value:    value,
			dataType: col.DataType,
		}
	}

	for i, cell := range row.ReturnCells {
		col, ok := dt.outputColumns[cell.Column]
		if !ok {
			return Row{}, fmt.Errorf("%w %q", ErrUnknownColumn, cell.Column)
		}
		value, err := sanitizeReturnValue(col.DataType, cell.Value)
		if err != nil {
			return Row{}, fmt.Errorf("return column %s: %w", col.Name, err)
		}
		prepared.ReturnCells[i] = ReturnCell{
			Column:   col.Name,
			Value:    value,
			dataType: col.DataType,
		}
	}

	return prepared, nil
}
