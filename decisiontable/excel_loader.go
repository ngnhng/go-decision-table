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
	"io"
	"strings"

	"github.com/xuri/excelize/v2"
)

const (
	excelSheetName       = "Decision Table"
	excelVersionRow      = 1
	excelMatchPolicyRow  = 2
	excelNoMatchRow      = 3
	excelColumnMarkerRow = 5
	excelFirstDataRow    = excelColumnMarkerRow + 4
	excelFirstColumn     = 2 // column B
	excelMaxColumns      = 1000
	excelMaxRows         = 10000
)

type excelColumnLayout struct {
	Conditions []Column
	Outputs    []Column
	Ordered    []Column
}

// LoadExcelFile loads a decision table from an Excel file that follows the legacy layout.
func LoadExcelFile(path string) (*DecisionTable, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("open excel file %s: %w", path, err)
	}
	defer f.Close()
	return loadExcelWorkbook(path, f)
}

// LoadExcel loads a decision table from an io.Reader (e.g., embedded resource).
func LoadExcel(name string, r io.Reader) (*DecisionTable, error) {
	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("open excel stream %s: %w", name, err)
	}
	defer f.Close()
	return loadExcelWorkbook(name, f)
}

func loadExcelWorkbook(name string, f *excelize.File) (*DecisionTable, error) {
	if _, err := f.GetSheetIndex(excelSheetName); err != nil {
		return nil, fmt.Errorf("sheet %q not found: %w", excelSheetName, err)
	}

	if _, err := expectLabelAndValue(f, excelVersionRow, "Version"); err != nil {
		return nil, err
	}

	matchPolicyRaw, err := expectLabelAndValue(f, excelMatchPolicyRow, "Match Policy")
	if err != nil {
		return nil, err
	}
	matchPolicy, err := parseMatchPolicyString(matchPolicyRaw)
	if err != nil {
		return nil, err
	}

	noMatchRaw, err := expectLabelAndValue(f, excelNoMatchRow, "No Match Policy")
	if err != nil {
		return nil, err
	}
	noMatchPolicy, err := parseNoMatchPolicyString(noMatchRaw)
	if err != nil {
		return nil, err
	}

	layout, firstCol, lastCol, err := readExcelColumns(f)
	if err != nil {
		return nil, err
	}

	dt, err := NewDecisionTable(name, layout.Conditions, layout.Outputs, WithMatchPolicy(matchPolicy), WithNoMatchPolicy(noMatchPolicy))
	if err != nil {
		return nil, err
	}

	rows, defaultRow, err := readExcelRows(f, layout, firstCol, lastCol)
	if err != nil {
		return nil, err
	}
	for idx, row := range rows {
		if err := dt.AddRow(row); err != nil {
			return nil, fmt.Errorf("row %d: %w", idx+1, err)
		}
	}
	if noMatchPolicy == NoMatchPolicyReturnDefault {
		if defaultRow == nil {
			return nil, fmt.Errorf("default row is required for RETURN_DEFAULT policy")
		}
		if err := dt.SetDefaultRow(*defaultRow); err != nil {
			return nil, err
		}
	}

	return dt, nil
}

func expectLabelAndValue(f *excelize.File, row int, label string) (string, error) {
	labelCell, err := f.GetCellValue(excelSheetName, cellName(1, row))
	if err != nil {
		return "", fmt.Errorf("read cell: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(labelCell), label) {
		return "", fmt.Errorf("expected %s marker in column A row %d", label, row)
	}
	value, err := f.GetCellValue(excelSheetName, cellName(2, row))
	if err != nil {
		return "", fmt.Errorf("read cell: %w", err)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s value missing in row %d", label, row)
	}
	return value, nil
}

func readExcelColumns(f *excelize.File) (excelColumnLayout, int, int, error) {
	marker, err := f.GetCellValue(excelSheetName, cellName(excelFirstColumn, excelColumnMarkerRow))
	if err != nil {
		return excelColumnLayout{}, 0, 0, err
	}
	if !strings.EqualFold(strings.TrimSpace(marker), "First Column") {
		return excelColumnLayout{}, 0, 0, fmt.Errorf("expected First Column marker at row %d column B", excelColumnMarkerRow)
	}

	lastCol := 0
	for col := excelFirstColumn + 1; col < excelFirstColumn+excelMaxColumns; col++ {
		value, err := f.GetCellValue(excelSheetName, cellName(col, excelColumnMarkerRow))
		if err != nil {
			return excelColumnLayout{}, 0, 0, err
		}
		if strings.TrimSpace(value) == "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(value), "Last Column") {
			return excelColumnLayout{}, 0, 0, fmt.Errorf("expected Last Column marker near row %d", excelColumnMarkerRow)
		}
		lastCol = col
		break
	}
	if lastCol == 0 {
		return excelColumnLayout{}, 0, 0, fmt.Errorf("could not find Last Column marker")
	}

	layout := excelColumnLayout{
		Conditions: []Column{},
		Outputs:    []Column{},
		Ordered:    []Column{},
	}
	seen := make(map[string]struct{})
	for col := excelFirstColumn; col <= lastCol; col++ {
		name, err := f.GetCellValue(excelSheetName, cellName(col, excelColumnMarkerRow+1))
		if err != nil {
			return excelColumnLayout{}, 0, 0, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return excelColumnLayout{}, 0, 0, fmt.Errorf("column name missing near row %d", excelColumnMarkerRow+1)
		}
		if _, exists := seen[name]; exists {
			return excelColumnLayout{}, 0, 0, fmt.Errorf("duplicate column %s", name)
		}
		seen[name] = struct{}{}

		typeRaw, err := f.GetCellValue(excelSheetName, cellName(col, excelColumnMarkerRow+2))
		if err != nil {
			return excelColumnLayout{}, 0, 0, err
		}
		colType, err := parseColumnTypeString(typeRaw)
		if err != nil {
			return excelColumnLayout{}, 0, 0, fmt.Errorf("column %s: %w", name, err)
		}

		dataTypeRaw, err := f.GetCellValue(excelSheetName, cellName(col, excelColumnMarkerRow+3))
		if err != nil {
			return excelColumnLayout{}, 0, 0, err
		}
		dataType, err := parseDataTypeString(dataTypeRaw)
		if err != nil {
			return excelColumnLayout{}, 0, 0, fmt.Errorf("column %s: %w", name, err)
		}
		column := Column{Name: name, Type: colType, DataType: dataType}
		layout.Ordered = append(layout.Ordered, column)
		switch colType {
		case ColumnTypeCondition:
			layout.Conditions = append(layout.Conditions, column)
		case ColumnTypeConclusion, ColumnTypeMetadata:
			layout.Outputs = append(layout.Outputs, column)
		}
	}

	if len(layout.Conditions) == 0 || len(layout.Outputs) == 0 {
		return excelColumnLayout{}, 0, 0, fmt.Errorf("excel table must define condition and output columns")
	}

	return layout, excelFirstColumn, lastCol, nil
}

func readExcelRows(f *excelize.File, layout excelColumnLayout, firstCol, lastCol int) ([]Row, *Row, error) {
	marker, err := f.GetCellValue(excelSheetName, cellName(1, excelFirstDataRow))
	if err != nil {
		return nil, nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(marker), "First Row") {
		return nil, nil, fmt.Errorf("expected First Row marker in column A row %d", excelFirstDataRow)
	}

	var rows []Row
	var defaultRow *Row
	ruleIDs := make(map[string]struct{})
	rowNumber := 0

	for rowIdx := excelFirstDataRow; rowIdx < excelFirstDataRow+excelMaxRows; rowIdx++ {
		rowNumber++
		row, err := convertExcelRow(f, layout, firstCol, lastCol, rowIdx, rowNumber, ruleIDs)
		if err != nil {
			return nil, nil, err
		}
		markerCell, err := f.GetCellValue(excelSheetName, cellName(1, rowIdx))
		if err != nil {
			return nil, nil, err
		}
		if strings.EqualFold(strings.TrimSpace(markerCell), "Default Row") {
			if len(row.EvalCells) > 0 {
				return nil, nil, fmt.Errorf("default row cannot contain evaluation cells")
			}
			defaultRow = &row
			break
		}
		rows = append(rows, row)
	}

	if defaultRow == nil {
		return nil, nil, fmt.Errorf("default row marker not found")
	}

	return rows, defaultRow, nil
}

func convertExcelRow(f *excelize.File, layout excelColumnLayout, firstCol, lastCol, rowIdx, rowNumber int, ruleIDs map[string]struct{}) (Row, error) {
	row := Row{Number: rowNumber}
	for col := firstCol; col <= lastCol; col++ {
		columnIndex := col - firstCol
		if columnIndex < 0 || columnIndex >= len(layout.Ordered) {
			return Row{}, fmt.Errorf("column index out of range")
		}
		column := layout.Ordered[columnIndex]
		rawValue, err := f.GetCellValue(excelSheetName, cellName(col, rowIdx))
		if err != nil {
			return Row{}, err
		}
		trimmed := strings.TrimSpace(rawValue)

		switch column.Type {
		case ColumnTypeCondition:
			if trimmed == "" {
				continue
			}
			op, operand, err := parseConditionString(trimmed, column.DataType)
			if err != nil {
				return Row{}, fmt.Errorf("column %s: %w", column.Name, err)
			}
			row.EvalCells = append(row.EvalCells, EvalCell{
				Column:   column.Name,
				Operator: op,
				Value:    operand,
			})
		case ColumnTypeConclusion, ColumnTypeMetadata:
			row.ReturnCells = append(row.ReturnCells, ReturnCell{
				Column: column.Name,
				Value:  rawValue,
			})
			if column.Type == ColumnTypeMetadata && trimmed != "" {
				if strings.EqualFold(column.Name, "ruleId") && row.RuleID == "" {
					row.RuleID = trimmed
				}
				if strings.EqualFold(column.Name, "description") && row.Comments == "" {
					row.Comments = trimmed
				}
			}
		}
	}

	if row.RuleID == "" {
		row.RuleID = fmt.Sprintf("%d", rowNumber)
	}
	if _, exists := ruleIDs[row.RuleID]; exists {
		return Row{}, fmt.Errorf("duplicate rule id %q", row.RuleID)
	}
	ruleIDs[row.RuleID] = struct{}{}

	return row, nil
}

func cellName(col, row int) string {
	name, _ := excelize.CoordinatesToCellName(col, row)
	return name
}

func parseConditionString(value string, dt DataType) (OperatorType, any, error) {
	delim := strings.Index(value, " ")
	if delim <= 0 {
		return "", nil, fmt.Errorf("invalid condition %q", value)
	}
	opToken := value[:delim]
	operand := strings.TrimSpace(value[delim+1:])
	if operand == "" {
		return "", nil, fmt.Errorf("missing operand for %q", opToken)
	}

	op, err := parseOperatorToken(opToken)
	if err != nil {
		return "", nil, err
	}

	if requiresCollectionValue(op) {
		values, err := splitAndDedupeList(operand)
		if err != nil {
			return "", nil, err
		}
		return op, values, nil
	}

	return op, operand, nil
}
