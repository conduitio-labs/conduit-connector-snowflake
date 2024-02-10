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
	"compress/gzip"
	"encoding/csv"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"golang.org/x/exp/maps"
)

func MakeCSVBytes(
	records []sdk.Record,
	schema map[string]string,
	prefix string,
	orderingColumns []string,
	insertsBuf *bytes.Buffer,
	updatesBuf *bytes.Buffer,
) ([]string, []string, error) {
	insertGzipWriter := gzip.NewWriter(insertsBuf)
	updateGzipWriter := gzip.NewWriter(updatesBuf)
	insertsWriter := csv.NewWriter(insertGzipWriter)
	updatesWriter := csv.NewWriter(updateGzipWriter)

	// we need to store the operation in a column, to detect updates & deletes
	operationColumn := fmt.Sprintf("%s_operation", prefix)
	schema[operationColumn] = "VARCHAR"
	csvColumnOrder := []string{operationColumn}
	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	for _, r := range records {
		// get Primary Key(s)
		if len(orderingColumns) == 0 {
			key, ok := r.Key.(sdk.StructuredData)
			if !ok {
				return nil, nil, errors.Errorf("key does not contain structured data (%T)", r.Key)
			}
			orderingColumns = maps.Keys(key)
		}

		data, err := extract(r.Operation, r.Payload)
		if err != nil {
			return nil, nil, errors.Errorf("failed to extract payload data: %w", err)
		}

		for key, val := range data {
			if schema[key] == "" {
				csvColumnOrder = append(csvColumnOrder, key)
				switch val.(type) {
				case int, int8, int16, int32, int64:
					schema[key] = "INTEGER"
				case float32, float64:
					schema[key] = "FLOAT"
				case bool:
					schema[key] = "BOOLEAN"
				case nil:
					// WE SHOULD KEEP TRACK OF VARIANTS SEPERATELY IN CASE WE RUN INTO CONCRETE TYPE LATER ON
					// IF WE RAN INTO NONE NULL VALUE OF THIS VARIANT COL, WE CAN EXECUTE AN ALTER TO DEST TABLE
					schema[key] = "VARIANT"
				default:
					schema[key] = "VARCHAR"
				}
			}
		}
	}

	numInserts, numUpdates, err := createCSVRecords(
		records,
		insertsWriter,
		updatesWriter,
		csvColumnOrder,
		operationColumn,
	)
	if err != nil {
		return nil, nil, errors.Errorf("could not create CSV records: %w", err)
	}

	if numInserts > 0 {
		insertsWriter.Flush()
		if err := insertsWriter.Error(); err != nil {
			return nil, nil, errors.Errorf("could not flush insertsWriter: %w", err)
		}

		if err := insertGzipWriter.Flush(); err != nil {
			return nil, nil, errors.Errorf("could not flush insertGzipWriter: %w", err)
		}
	
		if err := insertGzipWriter.Close(); err != nil { 
			return nil, nil, errors.Errorf("could not close insertGzipWriter: %w", err)
		}
	}

	if numUpdates > 0 {
		updatesWriter.Flush()
		if err := updatesWriter.Error(); err != nil {
			return nil, nil, errors.Errorf("could not flush updatesWriter: %w", err)
		}

		if err := updateGzipWriter.Flush(); err != nil {
			return nil, nil, errors.Errorf("could not flush updateGzipWriter: %w", err)
		}

		if err := updateGzipWriter.Close(); err != nil { 
			return nil, nil, errors.Errorf("could not close updateGzipWriter: %w", err)
		}
	}

	return orderingColumns, csvColumnOrder, nil
}

func createCSVRecords(records []sdk.Record, insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string, operationColumn string,
) (numInserts int, numUpdates int, err error) {
	var inserts, updates [][]string

	for _, r := range records {
		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload)
		if err != nil {
			return 0, 0, errors.Errorf("failed to extract payload data: %w", err)
		}

		for i, c := range csvColumnOrder {
			switch {
			case c == operationColumn:
				row[i] = r.Operation.String()
			case data[c] == nil:
				row[i] = ""
			default:
				row[i] = fmt.Sprint(data[c])
			}
		}

		switch r.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			inserts = append(inserts, row)
		case sdk.OperationUpdate, sdk.OperationDelete:
			updates = append(updates, row)
		default:
			return 0, 0, errors.Errorf("unexpected sdk.Operation: %s", r.Operation.String())
		}
	}

	if len(inserts) > 0 {
		if err := insertsWriter.Write(csvColumnOrder); err != nil {
			return 0, 0, errors.Errorf("failed to write insert headers: %w", err)
		}
		if err := insertsWriter.WriteAll(inserts); err != nil {
			return 0, 0, errors.Errorf("failed to write insert records: %w", err)
		}
	}

	if len(updates) > 0 {
		if err := updatesWriter.Write(csvColumnOrder); err != nil {
			return 0, 0, errors.Errorf("failed to write update headers: %w", err)
		}
		if err := updatesWriter.WriteAll(updates); err != nil {
			return 0, 0, errors.Errorf("failed to write update records: %w", err)
		}
	}

	return len(inserts), len(updates), nil
}

func extract(op sdk.Operation, payload sdk.Change) (sdk.StructuredData, error) {
	if op == sdk.OperationDelete {
		data, ok := payload.Before.(sdk.StructuredData)
		if !ok {
			return nil, errors.Errorf("payload.before does not contain structured data (%T)", payload.Before)
		}

		return data, nil
	}

	data, ok := payload.After.(sdk.StructuredData)
	if !ok {
		return nil, errors.Errorf("payload.after does not contain structured data (%T)", payload.After)
	}

	return data, nil
}
