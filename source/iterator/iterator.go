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

	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// action names.
	actionInsert = "insert"
)

// Iterator to iterate snowflake objects.
type Iterator struct {
	snowflake Repository

	table   string
	columns []string
	key     string

	index  int
	offset int
	limit  int

	data []map[string]interface{}
}

// New iterator.
func New(
	snowflake Repository,
	table string,
	columns []string,
	key string,
	index int,
	offset int,
	limit int,
	data []map[string]interface{},
) *Iterator {
	return &Iterator{
		snowflake: snowflake,
		table:     table,
		columns:   columns,
		key:       key,
		index:     index,
		offset:    offset,
		limit:     limit,
		data:      data,
	}
}

// HasNext check ability to get next record.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if i.index < len(i.data) {
		return true, nil
	}

	if i.index >= i.limit {
		i.offset += i.limit
		i.index = 0
	}

	i.data, err = i.snowflake.GetData(ctx, i.table, i.columns, i.offset, i.limit)
	if err != nil {
		return false, err
	}

	if len(i.data) == 0 || len(i.data) == i.index {
		return false, nil
	}

	return true, nil
}

// Next get new record.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		err     error
	)

	pos := position.Position{
		Element: i.index,
		Offset:  i.offset,
	}

	payload, err = json.Marshal(i.data[i.index])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %v", err)
	}

	if _, ok := i.data[i.index][strings.ToUpper(i.key)]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := i.data[i.index][strings.ToUpper(i.key)]

	i.index++

	return sdk.Record{
		Position: pos.FormatSDKPosition(),
		Metadata: map[string]string{
			metadataTable:  i.table,
			metadataAction: actionInsert,
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.key: key,
		},
		Payload: payload,
	}, nil
}

// Stop shutdown iterator.
func (i *Iterator) Stop() error {
	return i.snowflake.Close()
}

// Ack check if record with position was recorded.
func (i *Iterator) Ack(rp sdk.Position) error {
	p, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse sdk position: %v", err)
	}

	if p.Offset > i.offset || (p.Offset == i.offset && p.Element > i.index) {
		return fmt.Errorf("record was not recorded: element %d, offset %d", p.Element, p.Offset)
	}

	return nil
}
