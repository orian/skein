package api

import "encoding/json"

type JsonType int

const (
	JsonTypeInt JsonType = iota
	JsonTypeFloat
	JsonTypeString
	JsonTypeBool
	JsonTypeUnknown
)

type JSONArray struct {
	Types []JsonType
	Data  []interface{}
}

func (a *JSONArray) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	a.Data = make([]interface{}, len(a.Types))

	for i, typ := range a.Types {
		switch typ {
		case JsonTypeInt:
			var col []int64
			if err := json.Unmarshal(raw[i], &col); err != nil {
				return err
			}
			a.Data[i] = col
		case JsonTypeFloat:
			var col []float64
			if err := json.Unmarshal(raw[i], &col); err != nil {
				return err
			}
			a.Data[i] = col
		case JsonTypeString:
			var col []string
			if err := json.Unmarshal(raw[i], &col); err != nil {
				return err
			}
			a.Data[i] = col
		case JsonTypeBool:
			var col []bool
			if err := json.Unmarshal(raw[i], &col); err != nil {
				return err
			}
			a.Data[i] = col
		default:
			var col []interface{}
			if err := json.Unmarshal(raw[i], &col); err != nil {
				return err
			}
			a.Data[i] = col
		}
	}

	return nil
}
