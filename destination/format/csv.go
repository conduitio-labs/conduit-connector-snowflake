package format

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"golang.org/x/exp/maps"
)

// TODO Check mapping, we are assuming its structured atm

// OPTIMIZE THIS OMG
func MakeCSVRecords(records []sdk.Record, prefix string, orderingColumns []string) ([]byte, map[string]string, []string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// we need to store the operation in a column, to detect updates & deletes
	operationColumn := fmt.Sprintf("%s_operation", prefix)
	columnMap := map[string]string{operationColumn: "VARCHAR"}
	columnNames := []string{operationColumn}
	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	for _, r := range records {

		// get Primary Key(s)
		if len(orderingColumns) == 0 {
			var recordKeyMap map[string]interface{}
			// we are making an assumption here that it's structured data
			if err := json.Unmarshal(r.Key.Bytes(), &recordKeyMap); err != nil {
				return nil, nil, nil,
					errors.Errorf("could not unmarshal record.key, only structured data is supported: %w", err)
			}
			orderingColumns = maps.Keys(recordKeyMap)
		}

		var a sdk.Data
		//create a column map if we are updating or creating records
		if r.Operation != sdk.OperationDelete {
			a = r.Payload.After
		} else {
			a = r.Payload.Before
		}

		// infer ordering column from first record
		var cols map[string]interface{}

		// we are making an assumption here that it's structured data
		if err := json.Unmarshal(a.Bytes(), &cols); err != nil {
			return nil, nil, nil,
				errors.Errorf("could not unmarshal record.payload.after, only structured data is supported: %w", err)
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
		var a sdk.Data
		if val.Operation != sdk.OperationDelete {
			a = val.Payload.After
		} else {
			a = val.Payload.Before
		}

		if err := json.Unmarshal(a.Bytes(), &cols); err != nil {
			return nil, nil, nil,
				errors.Errorf("could not unmarshal record.payload.after, only structured data is supported: %w", err)
		}

		for _, c := range columnNames {
			if val.Operation == sdk.OperationDelete {
				fmt.Printf("@@@@@@@@ Value %s Column Name %s \n", fmt.Sprint(cols[c]), c)
			}
			if c == operationColumn {
				record = append(record, val.Operation.String())
				continue
			}
			switch cols[c].(type) {
			case nil:
				record = append(record, "")
			default:
				record = append(record, fmt.Sprint(cols[c]))
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

	return buf.Bytes(), columnMap, orderingColumns, nil
}
