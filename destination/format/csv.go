package format

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TODO Check mapping, we are assuming its structured atm

// OPTIMIZE THIS OMG
func MakeCSVRecords(records []sdk.Record) ([]byte, map[string]string, []string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	columnMap := map[string]string{}
	columnNames := []string{}
	for _, r := range records {
		var cols map[string]interface{}
		a := r.Payload.After
		if err := json.Unmarshal(a.Bytes(), &cols); err != nil {
			return nil, nil, nil, err
		}
		for key, val := range cols {
			if columnMap[key] == "" {
				columnNames = append(columnNames, key)
				switch val.(type) {
				case int, int8, int16, int32, int64:
					columnMap[key] = "INTEGER"
				case float32, float64:
					columnMap[key] = "INTEGER"
				case time.Time:
					columnMap[key] = "DATETIME"
				case bool:
					columnMap[key] = "BOOLEAN"
				case nil:
					//WE SHOULD KEEP TRACK OF VARIANTS SEPERATELY IN CASE WE RUN INTO CONCRETE TYPE LATER ON
					//IF WE RAN INTO NONE NULL VALUE OF THIS VARIANT COL, WE CAN EXECUTE AN ALTER TO DEST TABLE
					columnMap[key] = "VARIANT"
				default:
					columnMap[key] = "VARCHAR"
				}
			}
		}
	}
	writer.Write(columnNames)

	for _, val := range records {

		record := []string{}
		var cols map[string]interface{}
		a := val.Payload.After
		if err := json.Unmarshal(a.Bytes(), &cols); err != nil {
			return nil, nil, nil, err
		}

		for _, c := range columnNames {
			if cols[c] != nil {
				switch cols[c].(type) {
				case nil:
					record = append(record, "")
				default:
					record = append(record, fmt.Sprint(cols[c]))
				}
			}
		}

		err := writer.Write(record)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := writer.Error(); err != nil {
			return nil, nil, nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, nil, nil, err
	}

	return buf.Bytes(), columnMap, columnNames, nil
}
