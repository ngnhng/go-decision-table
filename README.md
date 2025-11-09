# Go Decision Table

The goal is to provide an idiomatic Go API for constructing in-memory decision tables and evaluating rules against input maps.

## Features

- Column metadata that matches the JSON DSL (`CONDITION`, `CONCLUSION`, `METADATA`) and supported data types (`STRING`, `INTEGER`, `DECIMAL`, `BOOLEAN`, `DATE`, `DATETIME`, `LIST_*`).
- Rich set of operators (`equal`, `greaterThan`, `in`, `anyContained`, `containsAll`, `allEqual`, …) with strict type coercion.
- Match policies (`FIRST`, `ALL`, `UNIQUE`) and no-match policies (`RETURN_DEFAULT`, `THROW_ERROR`).
- Strict row validation that sanitizes rule definitions before evaluation.
- JSON loader for the new DSL and an Excel loader that mirrors the same semantics.

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/ngnhng/go-decision-table/decisiontable"
)

func main() {
	evalCols := []decisiontable.Column{
		{Name: "age", Type: decisiontable.ColumnTypeCondition, DataType: decisiontable.DataTypeInteger},
		{Name: "country", Type: decisiontable.ColumnTypeCondition, DataType: decisiontable.DataTypeString},
	}
	retCols := []decisiontable.Column{
		{Name: "tier", Type: decisiontable.ColumnTypeConclusion, DataType: decisiontable.DataTypeString},
	}

	table, _ := decisiontable.NewDecisionTable(
		"eligibility",
		evalCols,
		retCols,
		decisiontable.WithMatchPolicy(decisiontable.MatchPolicyFirst),
		decisiontable.WithNoMatchPolicy(decisiontable.NoMatchPolicyReturnDefault),
	)

	_ = table.AddRow(decisiontable.Row{
		RuleID: "adult-us",
		EvalCells: []decisiontable.EvalCell{
			{Column: "age", Operator: decisiontable.OperatorGreaterOrEqual, Value: 18},
			{Column: "country", Operator: decisiontable.OperatorEqual, Value: "US"},
		},
		ReturnCells: []decisiontable.ReturnCell{
			{Column: "tier", Value: "standard"},
		},
	})

	_ = table.SetDefaultRow(decisiontable.Row{
		RuleID: "default",
		ReturnCells: []decisiontable.ReturnCell{
			{Column: "tier", Value: "minor"},
		},
	})

	rows, _ := table.Evaluate(map[string]any{"age": 25, "country": "US"}, nil)
	fmt.Println(rows[0].Values["tier"])
}
```

## How It Works

- **Column metadata**: Each condition column declares a data type and operator semantics. Values are sanitized (including JSON numbers, Excel strings, and precise `DECIMAL` values) before they are stored in the table.
- **Row preparation**: When you call `AddRow`, evaluation cells are parsed once (operators are resolved, `IN` lists are deduped) and return cells are coerced into their target type. Invalid columns or duplicate rule IDs fail fast.
- **Evaluation loop**: `Evaluate` walks the ordered rows, invoking `evaluateCell` for every condition cell until a mismatch is found. Depending on `MatchPolicy`, it stops at the first success or accumulates every match.
- **Return materialization**: Matching rows are converted into `MatchedRow` structs with cloned values so the caller can mutate the results without affecting the table. If no rows match, `NoMatchPolicy` decides whether to emit the configured default row or nothing at all.

## Loading from Files

```go
dtFromJSON, err := decisiontable.LoadJSONFile("rules/account.json")
dtFromExcel, err := decisiontable.LoadExcelFile("rules/account.xlsx")
```

Both follow the new JSON DSL semantics (match/no-match policies in the header, `CONDITION`/`CONCLUSION` column markers for Excel, `decisionTable` root object for JSON).

## Tests

Run the unit tests with:

```bash
go test ./...
```
