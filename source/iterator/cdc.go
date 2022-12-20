// Copyright © 2022 Meroxa, Inc.
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

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

// CDCIterator to iterate snowflake objects.
type CDCIterator struct {
	// repository for run queries to snowflake.
	snowflake Repository

	// table - table in snowflake for getting currentBatch.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// keys is the list of the column names that iterator use for setting key in record.
	keys []string

	// index - current index of element in current batch which iterator converts to record.
	index int
	// offset - current offset, show what batch iterator uses, using in query to get currentBatch.
	offset int
	// batchSize size of batch.
	batchSize int
	// currentBatch - rows in current batch from tracking table.
	currentBatch []map[string]interface{}
}

func NewCDCIterator(
	snowflake Repository,
	table string,
	keys, columns []string,
	index, offset, butchSize int,
	currentBatch []map[string]interface{},
) *CDCIterator {
	return &CDCIterator{
		snowflake:    snowflake,
		table:        table,
		columns:      columns,
		keys:         keys,
		index:        index,
		offset:       offset,
		batchSize:    butchSize,
		currentBatch: currentBatch,
	}
}

// HasNext check ability to get next record.
func (c *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	// Stream save two rows about update info:
	// 1) where metadata actionType = delete and metadata update = true, this is deleting.
	// 2) where metadata actionType = insertValue and update = true, this is exactly updating.
	// Skip first part and work only with second to avoid duplicate info.
	if c.index < len(c.currentBatch) && c.currentBatch[c.index][repository.MetadataColumnAction] == deleteValue &&
		c.currentBatch[c.index][repository.MetadataColumnUpdate] == true {
		c.index++

		return c.HasNext(ctx)
	}

	if c.index < len(c.currentBatch) {
		return true, nil
	}

	if c.index >= c.batchSize {
		c.offset += c.batchSize
		c.index = 0
	}

	c.currentBatch, err = c.snowflake.GetTrackingData(ctx, getStreamName(c.table),
		getTrackingTable(c.table), c.columns, c.offset, c.batchSize)
	if err != nil {
		// Snowflake library can return specific error for context cancel
		// Connector can't return this error and connector replace to
		// context cancel error
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return false, ctx.Err()
		}

		return false, err
	}

	if len(c.currentBatch) == 0 || len(c.currentBatch) == c.index {
		return false, nil
	}

	return true, nil
}

// Next get new record.
func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		err     error
		record  sdk.Record
	)

	pos := position.Position{
		IteratorType:             position.TypeCDC,
		SnapshotLastProcessedVal: nil,
		IndexInBatch:             c.index,
		BatchID:                  c.offset,
		Time:                     time.Now(),
	}

	action, err := getAction(c.currentBatch[c.index])
	if err != nil {
		return record, fmt.Errorf("get action: %w", err)
	}

	// remove metadata columns.
	delete(c.currentBatch[c.index], repository.MetadataColumnUpdate)
	delete(c.currentBatch[c.index], repository.MetadataColumnAction)
	delete(c.currentBatch[c.index], repository.MetadataColumnRow)
	delete(c.currentBatch[c.index], repository.MetadataColumnTime)

	payload, err = json.Marshal(c.currentBatch[c.index])
	if err != nil {
		return record, fmt.Errorf("marshal error : %w", err)
	}

	key := make(sdk.StructuredData)
	for i := range c.keys {
		val, ok := c.currentBatch[c.index][c.keys[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", c.keys[i])
		}

		key[c.keys[i]] = val
	}

	c.index++

	metadata := sdk.Metadata(map[string]string{metadataTable: c.table})
	metadata.SetCreatedAt(time.Now())

	p, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert to sdk position:%w", err)
	}

	switch action {
	case actionInsert:
		return sdk.Util.Source.NewRecordCreate(p, metadata, key, payload), nil
	case actionUpdate:
		return sdk.Util.Source.NewRecordUpdate(p, metadata, key, nil, payload), nil
	case actionDelete:
		return sdk.Util.Source.NewRecordDelete(p, metadata, key), nil
	default:
		return record, ErrCantFindActionType
	}
}

// Stop shutdown iterator.
func (c *CDCIterator) Stop() error {
	return c.snowflake.Close()
}

// Ack check if record with position was recorded.
func (c *CDCIterator) Ack(ctx context.Context, rp sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(rp)).Msg("got ack")

	return nil
}

func getStreamName(table string) string {
	return fmt.Sprintf(nameFormat, Conduit, "stream", table)
}

func getTrackingTable(table string) string {
	return fmt.Sprintf(nameFormat, Conduit, "tracking", table)
}

func getAction(data map[string]interface{}) (actionType, error) {
	if data[repository.MetadataColumnAction] == insertValue && data[repository.MetadataColumnUpdate] == false {
		return actionInsert, nil
	}

	if data[repository.MetadataColumnAction] == insertValue && data[repository.MetadataColumnUpdate] == true {
		return actionUpdate, nil
	}

	if data[repository.MetadataColumnAction] == deleteValue && data[repository.MetadataColumnUpdate] == false {
		return actionDelete, nil
	}

	return "", ErrCantFindActionType
}
