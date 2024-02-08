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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
)

func MakeCSVBytes(records []sdk.Record, prefix string, orderingColumns []string) (
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

	// write csv headers
	if err := insertsWriter.Write(csvColumnOrder); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := updatesWriter.Write(csvColumnOrder); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	if err := createCSVRecords(
		records,
		insertsWriter,
		updatesWriter,
		csvColumnOrder,
		operationColumn,
	); err != nil {
		return nil, nil, nil, nil, nil, errors.Errorf("could not create CSV records: %w", err)
	}

	return &insertsBuf, &updatesBuf, columnMap, orderingColumns, csvColumnOrder, nil
}

func createCSVRecords(records []sdk.Record, insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string, operationColumn string,
) error {
	var inserts, updates [][]string

	for _, r := range records {
		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload)
		if err != nil {
			return errors.Errorf("failed to extract payload data: %w", err)
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
			return errors.Errorf("unexpected sdk.Operation: %s", r.Operation.String())
		}
	}

	// N.B.: WriteAll will flush the buffer when comeplete.

	if err := insertsWriter.WriteAll(inserts); err != nil {
		return errors.Errorf("failed to write insert records: %w", err)
	}

	if err := updatesWriter.WriteAll(updates); err != nil {
		return errors.Errorf("failed to write update records: %w", err)
	}

	return nil
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
