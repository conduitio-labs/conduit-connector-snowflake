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
	"context"
	"encoding/csv"
	"fmt"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"golang.org/x/exp/maps"
)

const (
	isoFormat = `YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM`
)

type recordSummary struct{
	updatedAt string
	createdAt string
	deletedAt string
	latestRecord *sdk.Record
}

func MakeCSVBytes(
	ctx context.Context,
	records []sdk.Record,
	schema map[string]string,
	prefix string,
	primaryKey string,
	insertsBuf *bytes.Buffer,
	updatesBuf *bytes.Buffer,
	numGoroutines int,
) (columnOrder []string, err error) {
	insertGzipWriter := gzip.NewWriter(insertsBuf)
	updateGzipWriter := gzip.NewWriter(updatesBuf)
	insertsWriter := csv.NewWriter(insertGzipWriter)
	updatesWriter := csv.NewWriter(updateGzipWriter)

	// we need to store the operation in a column, to detect updates & deletes
	operationColumn := fmt.Sprintf("%s_operation", prefix)
	createdAtColumn := fmt.Sprintf("%s_created_at", prefix)
	updatedAtColumn := fmt.Sprintf("%s_updated_at", prefix)
	deletedAtColumn := fmt.Sprintf("%s_deleted_at", prefix)

	schema[operationColumn] = "VARCHAR"
	schema[createdAtColumn] = "TIMESTAMP_LTZ"
	schema[updatedAtColumn] = "TIMESTAMP_LTZ"
	schema[deletedAtColumn] = "TIMESTAMP_LTZ"

	csvColumnOrder := []string{operationColumn, createdAtColumn, updatedAtColumn, deletedAtColumn}
	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	// Grab the schema from the first record.
	// TODO: support schema evolution.
	if len(records) == 0 {
		return nil, errors.New("unexpected empty slice of records")
	}

	r := records[0]

	data, err := extract(r.Operation, r.Payload)
	if err != nil {
		return nil, errors.Errorf("failed to extract payload data: %w", err)
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

	// loop through records and de-dupe before converting to CSV
	// this is done beforehand, so we can parallelize the CSV formatting
	latestRecordMap := make(map[string]recordSummary, len(records))

	for _, r := range records {
		readAt := r.Metadata["opencdc.readAt"]
		s, err := extract(r.Operation, r.Payload)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprint(s[primaryKey]); 
		l, ok := latestRecordMap[key]
		if !ok {
			l.latestRecord = &r
		}

		switch r.Operation{
			case sdk.OperationUpdate:
				if l.updatedAt < readAt {
					l.updatedAt = readAt
					l.latestRecord = &r
				}
			case sdk.OperationDelete:
				l.deletedAt = readAt
				l.latestRecord = &r
			case sdk.OperationCreate, sdk.OperationSnapshot:
				l.createdAt = readAt
				l.latestRecord = &r
		}

		latestRecordMap[key] = l
	}

	// Process CSV records in parallel with goroutines
	var (
		wg                                 sync.WaitGroup
		insertsProcessed, updatesProcessed bool
	)
	insertsBuffers := make([]*bytes.Buffer, numGoroutines)
	updatesBuffers := make([]*bytes.Buffer, numGoroutines)
	errChan := make(chan error, numGoroutines)

	// number of records to process in each goroutine
	recordsPerRoutine := (len(latestRecordMap) + numGoroutines - 1) / numGoroutines
	sdk.Logger(ctx).Debug().Msgf("processing %d goroutines with %d records per routine",
		len(latestRecordMap), recordsPerRoutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			start := index * recordsPerRoutine
			end := start + recordsPerRoutine
			if end > len(latestRecordMap) {
				end = len(latestRecordMap)
			}

			// each goroutine gets its own set of buffers
			insertsBuffers[index] = new(bytes.Buffer)
			updatesBuffers[index] = new(bytes.Buffer)

			insertW := csv.NewWriter(insertsBuffers[index])
			updateW := csv.NewWriter(updatesBuffers[index])
			
			dedupedRecords := maps.Values(latestRecordMap)
			inserts, updates, err := createCSVRecords(ctx, dedupedRecords[start:end], insertW, updateW, csvColumnOrder, operationColumn, createdAtColumn, updatedAtColumn, deletedAtColumn)
			if err != nil {
				errChan <- errors.Errorf("failed to create CSV records: %w", err)
				return
			}

			if inserts > 0 {
				insertsProcessed = true
				insertW.Flush()
				if err := insertW.Error(); err != nil {
					errChan <- errors.Errorf("failed to flush inserts CSV writer: %w", err)
					return
				}
			}
			if updates > 0 {
				updatesProcessed = true
				updateW.Flush()
				if err := updateW.Error(); err != nil {
					errChan <- errors.Errorf("failed to flush updates CSV writer: %w", err)
					return
				}
			}
		}(i)
	}

	// wait for goroutines to complete
	wg.Wait()
	close(errChan)

	// check for errors from goroutines
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	if insertsProcessed {
		if err := insertsWriter.Write(csvColumnOrder); err != nil {
			return nil, errors.Errorf("failed to write insert headers: %w", err)
		}

		insertsWriter.Flush()
		if err := insertsWriter.Error(); err != nil {
			return nil, errors.Errorf("failed to flush insertsWriter: %w", err)
		}

		if err := joinBuffers(insertsBuffers, insertGzipWriter); err != nil {
			return nil, errors.Errorf("failed to join insert buffers: %w", err)
		}

		if err := insertGzipWriter.Flush(); err != nil {
			return nil, errors.Errorf("failed to flush insertGzipWriter: %w", err)
		}

		if err := insertGzipWriter.Close(); err != nil {
			return nil, errors.Errorf("failed to close insertGzipWriter: %w", err)
		}
	}

	if updatesProcessed {
		if err := updatesWriter.Write(csvColumnOrder); err != nil {
			return nil, errors.Errorf("failed to write update headers: %w", err)
		}

		updatesWriter.Flush()
		if err := updatesWriter.Error(); err != nil {
			return nil, errors.Errorf("failed to flush updatesWriter: %w", err)
		}

		if err := joinBuffers(updatesBuffers, updateGzipWriter); err != nil {
			return nil, errors.Errorf("failed to join update buffers: %w", err)
		}

		if err := updateGzipWriter.Flush(); err != nil {
			return nil, errors.Errorf("failed to flush updateGzipWriter: %w", err)
		}

		if err := updateGzipWriter.Close(); err != nil {
			return nil, errors.Errorf("failed to close updateGzipWriter: %w", err)
		}
	}

	return csvColumnOrder, nil
}

func createCSVRecords(ctx context.Context, recordSummaries []recordSummary, insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string, operationColumn, createdAtColumn, updatedAtColumn, deletedAtColumn string,
) (numInserts int, numUpdates int, err error) {

	var inserts, updates [][]string

	for _, s := range recordSummaries {
		if s.latestRecord == nil {
			continue
		}
		r := s.latestRecord

		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload)
		if err != nil {
			return 0, 0, errors.Errorf("failed to extract payload data: %w", err)
		}

		for j, c := range csvColumnOrder {
			switch {
			case c == operationColumn:
				row[j] = r.Operation.String()
			case c == createdAtColumn && r.Operation == sdk.OperationCreate:
				row[j] = s.createdAt
			case c == updatedAtColumn && r.Operation == sdk.OperationUpdate:
				row[j] = s.updatedAt
			case c == deletedAtColumn && r.Operation == sdk.OperationDelete:
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

func joinBuffers(buffers []*bytes.Buffer, w *gzip.Writer) error {
	for _, buf := range buffers {
		if _, err := buf.WriteTo(w); err != nil {
			w.Close()
			return err
		}
	}
	return nil
}
