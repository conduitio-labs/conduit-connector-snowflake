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
	"errors"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// Iterator to iterate snowflake objects.
type Iterator struct {
	snowflake Repository
	position  position.Position

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
	position position.Position,
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
		position:  position,
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

	if i.index == i.limit {
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
	pos := position.Position{
		Element: i.index,
		Offset:  i.offset,
	}

	payload := make(sdk.StructuredData)
	for k, v := range i.data[i.index] {
		payload[k] = v
	}

	if _, ok := i.data[i.index][i.key]; !ok {
		return sdk.Record{}, errors.New("key is not exist")
	}

	key := i.data[i.index][i.key]

	i.index++

	return sdk.Record{
		Position: pos.FormatSDKPosition(),
		Metadata: map[string]string{
			"table": i.table,
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
