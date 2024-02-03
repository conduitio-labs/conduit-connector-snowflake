// Copyright © 2024 Meroxa, Inc.
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

func makeCSVRecords(records []sdk.Record, prefix string, orderingColumns []string) (
	*bytes.Buffer, *bytes.Buffer, map[string]string, []string, []string, error,
) {
	var (
		insertsBuf bytes.Buffer
		updatesBuf bytes.Buffer
	)
	insertsWriter := csv.NewWriter(&insertsBuf)
	updatesWriter := csv.NewWriter(&updatesBuf)

	// we need to store the operation in a column, to detect updates & deletes
	operationColumn := fmt.Sprintf("%s_operation", prefix)
	columnMap := map[string]string{operationColumn: "VARCHAR"}
	csvColumnOrder := []string{operationColumn}
	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	for _, r := range records {
		// get Primary Key(s)
		if len(orderingColumns) == 0 {
			var recordKeyMap map[string]interface{}
			// we are making an assumption here that it's structured data
			if err := json.Unmarshal(r.Key.Bytes(), &recordKeyMap); err != nil {
				return nil, nil, nil, nil, nil,
					errors.Errorf("could not unmarshal record.key, only structured data is supported: %w", err)
			}
			orderingColumns = maps.Keys(recordKeyMap)
		}

		var a sdk.Data
		// create a column map if we are updating or creating records
		if r.Operation != sdk.OperationDelete {
			a = r.Payload.After
		} else {
			a = r.Payload.Before
		}

		// infer ordering column from first record
		var cols map[string]interface{}

		// we are making an assumption here that it's structured data
		if err := json.Unmarshal(a.Bytes(), &cols); err != nil {
			return nil, nil, nil, nil, nil,
				errors.Errorf("could not unmarshal record.payload.after, only structured data is supported: %w", err)
		}

		for key, val := range cols {
			if columnMap[key] == "" {
				csvColumnOrder = append(csvColumnOrder, key)
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
					// WE SHOULD KEEP TRACK OF VARIANTS SEPERATELY IN CASE WE RUN INTO CONCRETE TYPE LATER ON
					// IF WE RAN INTO NONE NULL VALUE OF THIS VARIANT COL, WE CAN EXECUTE AN ALTER TO DEST TABLE
					columnMap[key] = "VARIANT"
				default:
					columnMap[key] = "VARCHAR"
				}
			}
		}
	}

	// write csv headers
	if err := insertsWriter.Write(csvColumnOrder); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := updatesWriter.Write(csvColumnOrder); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	insertCount, updateCount, err := createCSVRecords(
		records,
		insertsWriter,
		updatesWriter,
		csvColumnOrder,
		operationColumn,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.Errorf("could not create CSV records: %w", err)
	}

	// If there are inserts, flush the writer. Otherwise, we should return with an empty buffer.
	if insertCount > 0 {
		insertsWriter.Flush()
		if err := insertsWriter.Error(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
	} else {
		insertsBuf = bytes.Buffer{}
	}

	// If there are no updates/deletes, empty the buffer to remove CSV headers
	if updateCount != 0 {
		updatesWriter.Flush()
		if err := updatesWriter.Error(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
	} else {
		updatesBuf = bytes.Buffer{}
	}

	return &insertsBuf, &updatesBuf, columnMap, orderingColumns, csvColumnOrder, nil
}

func createCSVRecords(records []sdk.Record, insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string, operationColumn string,
) (insertCount int, updateCount int, err error) {
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
			return 0, 0, errors.Errorf("could not unmarshal record.payload.after, only structured data is supported: %w", err)
		}

		for _, c := range csvColumnOrder {
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

		switch val.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			if err := insertsWriter.Write(record); err != nil {
				return 0, 0, err
			}
			insertCount++
		case sdk.OperationUpdate, sdk.OperationDelete:
			if err := updatesWriter.Write(record); err != nil {
				return 0, 0, err
			}
			updateCount++
		default:
			return 0, 0, errors.Errorf("unexpected sdk.Operation: %s", val.Operation.String())
		}
	}

	return insertCount, updateCount, err
}
