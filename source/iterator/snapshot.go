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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// SnapshotIterator to iterate snowflake objects.
type SnapshotIterator struct {
	// repository for run queries to snowflake.
	snowflake Repository

	// table - table in snowflake for getting data.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// Name of column what iterator use for setting key in record.
	key string

	// index - current index of element in current batch which iterator converts to record
	index int
	// offset - current offset, show what batch iterator uses, using in query to get data.
	offset int
	// batchSize size of batch.
	batchSize int

	// data - rows in current batch from table.
	data []map[string]interface{}
}

// NewSnapshotIterator iterator.
func NewSnapshotIterator(
	snowflake Repository,
	table string,
	columns []string,
	key string,
	index, offset, batchSize int,
	data []map[string]interface{},
) *SnapshotIterator {
	return &SnapshotIterator{
		snowflake: snowflake,
		table:     table,
		columns:   columns,
		key:       key,
		index:     index,
		offset:    offset,
		batchSize: batchSize,
		data:      data,
	}
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if i.index < len(i.data) {
		return true, nil
	}

	if i.index >= i.batchSize {
		i.offset += i.batchSize
		i.index = 0
	}

	i.data, err = i.snowflake.GetData(ctx, i.table, i.key, i.columns, i.offset, i.batchSize)
	if err != nil {
		return false, err
	}

	if len(i.data) == 0 || len(i.data) <= i.index {
		return false, nil
	}

	return true, nil
}

// Next get new record.
func (i *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		err     error
	)

	pos := position.NewPosition(position.TypeSnapshot, i.index, i.offset)

	payload, err = json.Marshal(i.data[i.index])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	if _, ok := i.data[i.index][i.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := i.data[i.index][i.key]

	i.index++

	return sdk.Record{
		Position: pos.ConvertToSDKPosition(),
		Metadata: map[string]string{
			metadataTable:  i.table,
			metadataAction: string(actionInsert),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.key: key,
		},
		Payload: payload,
	}, nil
}

// Stop shutdown iterator.
func (i *SnapshotIterator) Stop() error {
	return i.snowflake.Close()
}

// Ack check if record with position was recorded.
func (i *SnapshotIterator) Ack(rp sdk.Position) error {
	p, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse sdk position: %w", err)
	}

	if p.BatchID > i.offset || (p.BatchID == i.offset && p.IndexInBatch > i.index) {
		return fmt.Errorf("record with this postiton was not recorded: "+
			"element %d, offset %d", p.IndexInBatch, p.BatchID)
	}

	return nil
}
