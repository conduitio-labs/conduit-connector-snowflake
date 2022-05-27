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

	"github.com/conduitio/conduit-connector-snowflake/repository"
	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// CDCIterator to iterate snowflake objects.
type CDCIterator struct {
	snowflake Repository

	table   string
	columns []string
	key     string

	index     int
	offset    int
	butchSize int

	data []map[string]interface{}
}

func NewCDCIterator(
	snowflake Repository,
	table string,
	columns []string,
	key string,
	index, offset, butchSize int,
	data []map[string]interface{},
) *CDCIterator {
	return &CDCIterator{
		snowflake: snowflake,
		table:     table,
		columns:   columns,
		key:       key,
		index:     index,
		offset:    offset,
		butchSize: butchSize,
		data:      data,
	}
}

// HasNext check ability to get next record.
func (c *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	// Stream save two rows about update info:
	// 1) where metadata actionType = delete and metadata update = true, this is deleting.
	// 2) where metadata actionType = insertValue and update = true, this is exactly updating.
	// Skip first part and work only with second to avoid duplicate info.
	if c.index < len(c.data) && c.data[c.index][repository.MetadataColumnAction] == deleteValue &&
		c.data[c.index][repository.MetadataColumnUpdate] == true {
		c.index++

		return c.HasNext(ctx)
	}

	if c.index < len(c.data) {
		return true, nil
	}

	if c.index >= c.butchSize {
		c.offset += c.butchSize
		c.index = 0
	}

	c.data, err = c.snowflake.GetTrackingData(ctx, getStreamName(c.table),
		getTrackingTable(c.table), c.columns, c.offset, c.butchSize)
	if err != nil {
		return false, err
	}

	if len(c.data) == 0 || len(c.data) == c.index {
		return false, nil
	}

	return true, nil
}

// Next get new record.
func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		err     error
	)

	pos := position.NewPosition(position.TypeCDC, c.index, c.offset)

	action, err := getAction(c.data[c.index])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get action: %w", err)
	}

	// remove metadata columns.
	delete(c.data[c.index], repository.MetadataColumnUpdate)
	delete(c.data[c.index], repository.MetadataColumnAction)
	delete(c.data[c.index], repository.MetadataColumnRow)
	delete(c.data[c.index], repository.MetadataColumnTime)

	payload, err = json.Marshal(c.data[c.index])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	if _, ok := c.data[c.index][c.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := c.data[c.index][c.key]

	c.index++

	return sdk.Record{
		Position: pos.ConvertToSDKPosition(),
		Metadata: map[string]string{
			metadataTable:  c.table,
			metadataAction: string(action),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			c.key: key,
		},
		Payload: payload,
	}, nil
}

// Stop shutdown iterator.
func (c *CDCIterator) Stop() error {
	return c.snowflake.Close()
}

// Ack check if record with position was recorded.
func (c *CDCIterator) Ack(rp sdk.Position) error {
	p, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse sdk position: %w", err)
	}

	if p.Offset > c.offset || (p.Offset == c.offset && p.Element > c.index) {
		return fmt.Errorf("record was not recorded: element %d, offset %d", p.Element, p.Offset)
	}

	return nil
}

func getStreamName(table string) string {
	return fmt.Sprintf(nameFormat, conduit, "stream", table)
}

func getTrackingTable(table string) string {
	return fmt.Sprintf(nameFormat, conduit, "tracking", table)
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
