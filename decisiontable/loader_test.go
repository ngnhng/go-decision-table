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
	"math/big"
	"path/filepath"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestLoadJSON(t *testing.T) {
	const doc = `
{
  "decisionTable": {
    "name": "loanEligibilityCheck",
    "policies": {
      "matchPolicy": "FIRST",
      "noMatchPolicy": "RETURN_DEFAULT"
    },
    "columns": [
      {"name": "creditScore", "type": "CONDITION", "dataType": "INTEGER"},
      {"name": "dtiRatio", "type": "CONDITION", "dataType": "DECIMAL"},
      {"name": "customerCategory", "type": "CONDITION", "dataType": "STRING"},
      {"name": "isApproved", "type": "CONCLUSION", "dataType": "BOOLEAN"},
      {"name": "interestRate", "type": "CONCLUSION", "dataType": "DECIMAL"},
      {"name": "ruleId", "type": "METADATA", "dataType": "STRING"}
    ],
    "rules": [
      {
        "id": "rule-001-premium",
        "description": "Excellent credit score and low DTI for premium customers.",
        "when": [
          {"operator": "greaterThanOrEqual", "value": 780},
          {"operator": "lessThan", "value": 0.3},
          {"operator": "in", "value": ["PREMIUM", "VIP"]}
        ],
        "then": [true, 3.5, "rule-001-premium"]
      },
      {
        "id": "rule-002-good",
        "description": "Good credit score and acceptable DTI.",
        "when": [
          {"operator": "greaterThanOrEqual", "value": 700},
          {"operator": "lessThanOrEqual", "value": 0.4},
          {}
        ],
        "then": [true, 4.8, "rule-002-good"]
      }
    ],
    "defaultRule": {
      "description": "Default rejection for all other cases.",
      "then": [false, null, "default-rejection"]
    }
  }
}`

	dt, err := LoadJSON([]byte(doc), "eligibility.json")
	if err != nil {
		t.Fatalf("load json: %v", err)
	}

	rows, err := dt.Evaluate(map[string]any{
		"creditScore":      790,
		"dtiRatio":         0.25,
		"customerCategory": "VIP",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].RuleID != "rule-001-premium" {
		t.Fatalf("unexpected rule id %s", rows[0].RuleID)
	}
	if approved, ok := rows[0].Values["isApproved"].(bool); !ok || !approved {
		t.Fatalf("expected approval to be true, got %#v", rows[0].Values["isApproved"])
	}
	if rate, ok := rows[0].Values["interestRate"].(*big.Float); !ok || rate.Cmp(big.NewFloat(3.5)) != 0 {
		t.Fatalf("unexpected interest rate %#v", rows[0].Values["interestRate"])
	}
	if id, ok := rows[0].Values["ruleId"].(string); !ok || id != "rule-001-premium" {
		t.Fatalf("unexpected metadata rule id %#v", rows[0].Values["ruleId"])
	}

	rows, err = dt.Evaluate(map[string]any{
		"creditScore":      640,
		"dtiRatio":         0.6,
		"customerCategory": "STANDARD",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected default row, got %#v", rows)
	}
	if rows[0].Values["isApproved"] != false {
		t.Fatalf("expected default approval false, got %#v", rows[0].Values["isApproved"])
	}
	if rows[0].Values["interestRate"] != nil {
		t.Fatalf("expected default interest rate nil, got %#v", rows[0].Values["interestRate"])
	}
	if rows[0].Values["ruleId"] != "default-rejection" {
		t.Fatalf("unexpected default metadata %#v", rows[0].Values["ruleId"])
	}
}

func TestLoadJSONNoMatchThrowsError(t *testing.T) {
	const doc = `
{
  "decisionTable": {
    "name": "scoreCheck",
    "policies": {
      "matchPolicy": "ALL",
      "noMatchPolicy": "THROW_ERROR"
    },
    "columns": [
      {"name": "score", "type": "CONDITION", "dataType": "INTEGER"},
      {"name": "segment", "type": "CONCLUSION", "dataType": "STRING"}
    ],
    "rules": [
      {
        "id": "rule-1",
        "when": [
          {"operator": "greaterThan", "value": 50}
        ],
        "then": ["high"]
      }
    ]
  }
}`

	dt, err := LoadJSON([]byte(doc), "score.json")
	if err != nil {
		t.Fatalf("load json: %v", err)
	}
	if _, err := dt.Evaluate(map[string]any{"score": 10}, nil); err == nil {
		t.Fatalf("expected error for missing match")
	}
}

func TestLoadExcel(t *testing.T) {
	path := buildExcelFixture(t)
	dt, err := LoadExcelFile(path)
	if err != nil {
		t.Fatalf("load excel: %v", err)
	}

	rows, err := dt.Evaluate(map[string]any{
		"age":     25,
		"country": "US",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].RuleID != "row1" {
		t.Fatalf("unexpected excel match: %#v", rows)
	}

	rows, err = dt.Evaluate(map[string]any{
		"age":     12,
		"country": "FR",
	}, nil)
	if err != nil {
		t.Fatalf("evaluate returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].Values["tier"] != "minor" {
		t.Fatalf("expected default row from excel, got %#v", rows)
	}
}

func buildExcelFixture(t *testing.T) string {
	t.Helper()
	f := excelize.NewFile()
	sheet := f.GetSheetName(0)
	f.SetSheetName(sheet, excelSheetName)

	set := func(cell, value string) {
		if err := f.SetCellValue(excelSheetName, cell, value); err != nil {
			t.Fatalf("set cell %s: %v", cell, err)
		}
	}

	set("A1", "Version")
	set("B1", "1.0")
	set("A2", "Match Policy")
	set("B2", "ALL")
	set("A3", "No Match Policy")
	set("B3", "RETURN_DEFAULT")

	set("B5", "First Column")
	set("F5", "Last Column")

	// column definitions
	headers := []struct {
		col string
	}{
		{"B"},
		{"C"},
		{"D"},
		{"E"},
		{"F"},
	}
	names := []string{"age", "country", "tier", "ruleId", "comments"}
	types := []string{"Condition", "Condition", "Conclusion", "Metadata", "Metadata"}
	dataTypes := []string{"Integer", "String", "String", "String", "String"}
	for idx, header := range headers {
		set(header.col+"6", names[idx])
		set(header.col+"7", types[idx])
		set(header.col+"8", dataTypes[idx])
	}

	// rows
	set("A9", "First Row")
	set("B9", ">= 18")
	set("C9", "= US")
	set("D9", "standard")
	set("E9", "row1")
	set("F9", "adult rule")

	set("B10", ">= 21")
	set("C10", "IN CA,MX")
	set("D10", "neighbor")
	set("E10", "row2")
	set("F10", "neighbor rule")

	set("A11", "Default Row")
	set("D11", "minor")
	set("E11", "default")
	set("F11", "default row")

	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.xlsx")
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save excel: %v", err)
	}
	return path
}
