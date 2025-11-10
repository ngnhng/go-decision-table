# Go Decision Table

The goal is to provide an idiomatic Go API for constructing in-memory decision tables and evaluating rules against input maps.

## DSL

- [JSON DSL Spec](https://github.com/ngnhng/go-decision-table/wiki/JSON-DSL-Specification-Draft)
- TODO: Excel Spec

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

- **Describe your columns**: List every condition and conclusion column with its semantic type so the table knows how to compare and return values.
- **Load the rules**: Each row pairs operators (`GT`, `IN`, `ANY_CONTAINED_IN`, …) with values. When rows are added they’re validated once, so typos or unsupported data types fail fast instead of at runtime.
- **Evaluate in order**: `Evaluate` walks the rows top-to-bottom, stopping at the first match, collecting all matches, or enforcing uniqueness depending on the selected match policy.
- **Handle defaults**: No-match policies decide whether to surface a custom default row, return a caller-provided fallback, or error out when nothing applies.

### Example flow

Imagine the credit-policy table from the docs:

1. Applicant (810 score, 0.27 DTI, VIP) satisfies the premium rule (`>=780`, `<0.30`, category in `["PREMIUM","VIP"]`) and immediately receives approval at 3.5%.
2. Another applicant (735 score, 0.33 DTI) skips the premium rule but matches the “good” row, so `Evaluate` returns approval at 4.8%.
3. Someone with 0.55 DTI hits the high-risk rule and is rejected.
4. Everyone else falls through to the default-rejection row defined in the table.

That same JSON/Excel definition can be loaded directly, or you can recreate it programmatically as shown in the quick start.

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
