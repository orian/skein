package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testData = `{
	"column_names": ["a", "b", "c"],
	"column_types": [{"type":"BIGINT"}, {"type":"TEXT"}, {"type":"BOOLEAN"}, {"type":"FLOAT"}],
	"column_data": [
		[1, 2, 3],
		["foo", "bar", "baz"],
		[true, false, true],
		[1.2, 3.4, 5.6]
	]
}`

func TestInternalJobResult_UnmarshalJSON(t *testing.T) {
	var got JobResult
	if err := json.Unmarshal([]byte(testData), &got); err != nil {
		t.Fatalf("Failed to unmarshal test data: %v", err)
	}

	want := JobResult{
		ColumnNames: []string{"a", "b", "c"},
		ColumnTypes: []ColumnType{{Type: "BIGINT"}, {Type: "TEXT"}, {Type: "BOOLEAN"}, {Type: "FLOAT"}},
		ColumnData:  []interface{}{[]int64{1, 2, 3}, []string{"foo", "bar", "baz"}, []bool{true, false, true}, []float32{1.2, 3.4, 5.6}},
	}
	assert.Equal(t, want, got, "expected empty result")
}
