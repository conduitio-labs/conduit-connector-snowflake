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
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"golang.org/x/exp/maps"
)

const (
	snowflakeTimeStamp = "TIMESTAMP_LTZ"
	snowflakeVarchar   = "VARCHAR"
	snowflakeVariant   = "VARIANT"
	snowflakeInteger   = "INTEGER"
	snowflakeBoolean   = "BOOLEAN"
	snowflakeFloat     = "FLOAT"
)

type recordSummary struct {
	updatedAt    string
	createdAt    string
	deletedAt    string
	latestRecord *sdk.Record
}

type ConnectorColumns struct {
	operationColumn string
	createdAtColumn string
	updatedAtColumn string
	deletedAtColumn string
}

func GetDataSchema(
	ctx context.Context,
	records []sdk.Record,
	schema map[string]string,
	prefix string,
) ([]string, *ConnectorColumns, error) {
	// we need to store the operation in a column, to detect updates & deletes
	connectorColumns := ConnectorColumns{
		operationColumn: fmt.Sprintf("%s_operation", prefix),
		createdAtColumn: fmt.Sprintf("%s_created_at", prefix),
		updatedAtColumn: fmt.Sprintf("%s_updated_at", prefix),
		deletedAtColumn: fmt.Sprintf("%s_deleted_at", prefix),
	}

	schema[connectorColumns.operationColumn] = snowflakeVarchar
	schema[connectorColumns.createdAtColumn] = snowflakeTimeStamp
	schema[connectorColumns.updatedAtColumn] = snowflakeTimeStamp
	schema[connectorColumns.deletedAtColumn] = snowflakeTimeStamp

	csvColumnOrder := []string{}

	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	// Grab the schema from the first record.
	// TODO: support schema evolution.
	if len(records) == 0 {
		return nil, nil, errors.New("unexpected empty slice of records")
	}

	r := records[0]
	data, err := extract(r.Operation, r.Payload, r.Key)
	if err != nil {
		return nil, nil, errors.Errorf("failed to extract payload data: %w", err)
	}

	for key, val := range data {
		if schema[key] == "" {
			csvColumnOrder = append(csvColumnOrder, key)
			switch val.(type) {
			case int, int8, int16, int32, int64:
				schema[key] = snowflakeInteger
			case float32, float64:
				schema[key] = snowflakeFloat
			case bool:
				schema[key] = snowflakeBoolean
			case nil:
				// WE SHOULD KEEP TRACK OF VARIANTS SEPERATELY IN CASE WE RUN INTO CONCRETE TYPE LATER ON
				// IF WE RAN INTO NONE NULL VALUE OF THIS VARIANT COL, WE CAN EXECUTE AN ALTER TO DEST TABLE
				schema[key] = snowflakeVariant
			default:
				schema[key] = snowflakeVarchar
			}
		}
	}

	// sort data column order alphabetically to make deterministic
	// but keep conduit connector columns at the front for ease of use
	sort.Strings(csvColumnOrder)
	csvColumnOrder = append(
		[]string{
			connectorColumns.operationColumn,
			connectorColumns.createdAtColumn,
			connectorColumns.updatedAtColumn,
			connectorColumns.deletedAtColumn,
		},
		csvColumnOrder...,
	)

	sdk.Logger(ctx).Debug().Msgf("schema detected: %+v", schema)

	return csvColumnOrder, &connectorColumns, nil
}

//nolint:gocyclo // TODO: refactor this function, make it more modular and readable.
func MakeCSVBytes(
	ctx context.Context,
	records []sdk.Record,
	csvColumnOrder []string,
	meroxaColumns ConnectorColumns,
	primaryKey string,
	insertsBuf *bytes.Buffer,
	updatesBuf *bytes.Buffer,
	numGoroutines int,
) (err error) {
	insertsWriter := csv.NewWriter(insertsBuf)
	updatesWriter := csv.NewWriter(updatesBuf)

	// loop through records and de-dupe before converting to CSV
	// this is done beforehand, so we can parallelize the CSV formatting
	latestRecordMap := make(map[string]*recordSummary, len(records))
	sdk.Logger(ctx).Debug().Msgf("num of records in batch before deduping: %d", len(records))

	for i, r := range records {
		readAt := r.Metadata["opencdc.readAt"]
		s, err := extract(r.Operation, r.Payload, r.Key)
		key := fmt.Sprint(s[primaryKey])
		if err != nil {
			return err
		}

		l, ok := latestRecordMap[key]
		if !ok {
			l = &recordSummary{
				latestRecord: &records[i],
			}
			latestRecordMap[key] = l
		}

		switch r.Operation {
		case sdk.OperationUpdate:
			if l.updatedAt < readAt {
				l.updatedAt = readAt
				l.latestRecord = &records[i]
			}
		case sdk.OperationDelete:
			l.deletedAt = readAt
			l.latestRecord = &records[i]
		case sdk.OperationCreate, sdk.OperationSnapshot:
			l.createdAt = readAt
			l.latestRecord = &records[i]
		}
	}

	// Process CSV records in parallel with goroutines
	var wg sync.WaitGroup
	insertsBuffers := make([]*bytes.Buffer, numGoroutines)
	updatesBuffers := make([]*bytes.Buffer, numGoroutines)
	errChan := make(chan error, numGoroutines)
	insertsProcessedChan := make(chan bool, numGoroutines)
	updatesProcessedChan := make(chan bool, numGoroutines)

	dedupedRecords := maps.Values(latestRecordMap)
	recordChunks := splitRecordChunks(dedupedRecords, numGoroutines)

	sdk.Logger(ctx).Debug().Msgf("num of records in batch after deduping: %d", len(dedupedRecords))
	sdk.Logger(ctx).Debug().Msgf("processing %d goroutines in %d chunks",
		numGoroutines, len(recordChunks))

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// each goroutine gets its own set of buffers
			insertsBuffers[index] = new(bytes.Buffer)
			updatesBuffers[index] = new(bytes.Buffer)

			insertW := csv.NewWriter(insertsBuffers[index])
			updateW := csv.NewWriter(updatesBuffers[index])

			inserts, updates, err := createCSVRecords(ctx,
				recordChunks[index],
				insertW,
				updateW,
				csvColumnOrder,
				meroxaColumns)
			if err != nil {
				errChan <- errors.Errorf("failed to create CSV records: %w", err)

				return
			}

			if inserts > 0 {
				insertsProcessedChan <- true
			}

			if updates > 0 {
				updatesProcessedChan <- true
			}
		}(i)
	}

	// wait for goroutines to complete
	wg.Wait()
	close(errChan)
	close(insertsProcessedChan)
	close(updatesProcessedChan)

	// check for errors from goroutines
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	var insertsProcessed, updatesProcessed bool
	for p := range insertsProcessedChan {
		insertsProcessed = insertsProcessed || p
	}
	for p := range updatesProcessedChan {
		updatesProcessed = updatesProcessed || p
	}

	if insertsProcessed {
		if err := insertsWriter.Write(csvColumnOrder); err != nil {
			return errors.Errorf("failed to write insert headers: %w", err)
		}

		if err := joinBuffers(insertsBuffers, insertsBuf); err != nil {
			return errors.Errorf("failed to join insert buffers: %w", err)
		}
	}

	if updatesProcessed {
		if err := updatesWriter.Write(csvColumnOrder); err != nil {
			return errors.Errorf("failed to write update headers: %w", err)
		}

		if err := joinBuffers(updatesBuffers, updatesBuf); err != nil {
			return errors.Errorf("failed to join update buffers: %w", err)
		}
	}

	return nil
}

func createCSVRecords(
	_ context.Context,
	recordSummaries []*recordSummary,
	insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string,
	m ConnectorColumns,
) (numInserts int, numUpdates int, err error) {
	var inserts, updates [][]string

	for _, s := range recordSummaries {
		if s.latestRecord == nil {
			continue
		}
		r := s.latestRecord

		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload, r.Key)
		if err != nil {
			return 0, 0, errors.Errorf("failed to extract payload data: %w", err)
		}

		for j, c := range csvColumnOrder {
			switch {
			case c == m.operationColumn:
				row[j] = r.Operation.String()
			case c == m.createdAtColumn && (r.Operation == sdk.OperationCreate || r.Operation == sdk.OperationSnapshot):
				row[j] = s.createdAt
			case c == m.updatedAtColumn && r.Operation == sdk.OperationUpdate:
				row[j] = s.updatedAt
			case c == m.deletedAtColumn && r.Operation == sdk.OperationDelete:
				row[j] = s.deletedAt
			case data[c] == nil:
				row[j] = ""
			default:
				row[j] = fmt.Sprint(data[c])
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
		if err := insertsWriter.WriteAll(inserts); err != nil {
			return 0, 0, errors.Errorf("failed to write insert records: %w", err)
		}
	}

	if len(updates) > 0 {
		if err := updatesWriter.WriteAll(updates); err != nil {
			return 0, 0, errors.Errorf("failed to write update records: %w", err)
		}
	}

	return len(inserts), len(updates), nil
}

func extract(op sdk.Operation, payload sdk.Change, key sdk.Data) (sdk.StructuredData, error) {
	var sdkData sdk.Data
	if op == sdk.OperationDelete {
		sdkData = payload.Before
	} else {
		sdkData = payload.After
	}

	dataStruct, okStruct := sdkData.(sdk.StructuredData)
	dataRaw, okRaw := sdkData.(sdk.RawData)

	if !okStruct && !okRaw {
		dataStruct, okStruct = key.(sdk.StructuredData)
		dataRaw, okRaw = key.(sdk.RawData)
		if !okRaw && !okStruct {
			return nil, errors.Errorf("cannot find data either in payload (%T) or key (%T)", sdkData, key)
		}
		sdkData = key
	}

	if okStruct {
		return dataStruct, nil
	} else if okRaw {
		data := make(sdk.StructuredData)
		if err := json.Unmarshal(dataRaw, &payload); err != nil {
			return nil, errors.Errorf("cannot unmarshal raw data into structured (%T)", sdkData)
		}

		return data, nil
	}

	return nil, errors.Errorf("data payload does not contain structured or raw data (%T)", sdkData)
}

func joinBuffers(buffers []*bytes.Buffer, w *bytes.Buffer) error {
	var bufsize int
	for _, b := range buffers {
		bufsize += b.Len()
	}

	w.Grow(bufsize)

	for _, b := range buffers {
		if _, err := b.WriteTo(w); err != nil {
			return err
		}
	}

	return nil
}

// splitRecordChunks takes a slice of recordSummary and an integer n, then splits the slice into n chunks.
// Note: The last chunk may have fewer elements if the slice size is not evenly divisible by n.
// TODO: replace this with.
func splitRecordChunks(slice []*recordSummary, n int) [][]*recordSummary {
	var chunks [][]*recordSummary

	// Calculate chunk size
	totalLen := len(slice)
	if n <= 0 {
		n = 1 // Ensure there is at least one chunk
	}
	chunkSize := totalLen / n
	remainder := totalLen % n

	start := 0
	for i := 0; i < n; i++ {
		end := start + chunkSize
		if i < remainder {
			end++ // Distribute the remainder among the first few chunks
		}

		// Adjust end if it goes beyond the slice length
		if end > totalLen {
			end = totalLen
		}

		chunks = append(chunks, slice[start:end])
		start = end

		// Break the loop early if we've already included all elements
		if start >= totalLen {
			break
		}
	}

	return chunks
}
