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

	"github.com/conduitio/conduit-connector-snowflake/repository"
	p "github.com/conduitio/conduit-connector-snowflake/source/position/cdc"
)

const (
	Conduit = "conduit"
)

// CDCIterator to iterate snowflake objects.
type CDCIterator struct {
	snowflake Repository

	table   string
	columns []string
	key     string

	indexInsertElement int
	indexUpdateElement int
	indexDeleteElement int

	action action

	insertData []map[string]interface{}
	updateData []map[string]interface{}
	deleteData []map[string]interface{}
}

func NewCDCIterator(
	snowflake Repository,
	table, key string,
	columns []string,
	indexInsertElement int,
	indexUpdateElement int,
	indexDeleteElement int,
) *CDCIterator {
	return &CDCIterator{
		snowflake:          snowflake,
		table:              table,
		columns:            columns,
		key:                key,
		indexInsertElement: indexInsertElement,
		indexUpdateElement: indexUpdateElement,
		indexDeleteElement: indexDeleteElement,
	}
}

// HasNext check ability to get next record.
func (c *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	if c.hasData() {
		return true, nil
	}

	data, err := c.snowflake.GetStreamData(ctx, getStreamName(c.table), c.columns)
	if err != nil {
		return false, fmt.Errorf("get data: %v", err)
	}

	c.filterData(data)

	return c.hasData(), nil
}

// Next get new record.
func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		data    map[string]interface{}
		err     error
	)

	switch c.action {
	case actionInsert:
		data = c.insertData[c.indexInsertElement]
		c.indexInsertElement++
	case actionDelete:
		data = c.deleteData[c.indexDeleteElement]
		c.indexDeleteElement++
	case actionUpdate:
		data = c.updateData[c.indexUpdateElement]
		c.indexUpdateElement++
	default:
		return sdk.Record{}, ErrUnknownAction
	}

	pos := p.NewPosition(c.indexInsertElement, c.indexUpdateElement, c.indexDeleteElement)

	// remove metadata columns.
	delete(data, repository.MetadataColumnUpdate)
	delete(data, repository.MetadataColumnAction)
	delete(data, repository.MetadataColumnRow)

	payload, err = json.Marshal(data)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %v", err)
	}

	if _, ok := data[c.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := data[strings.ToUpper(c.key)]

	return sdk.Record{
		Position: pos.FormatSDKPosition(),
		Metadata: map[string]string{
			metadataTable:  c.table,
			metadataAction: string(c.action),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			c.key: key,
		},
		Payload: payload,
	}, nil
}

func (c *CDCIterator) hasActionData(action action) bool {
	if action == actionInsert && len(c.insertData) > c.indexInsertElement {
		return true
	}

	if action == actionUpdate && len(c.updateData) > c.indexUpdateElement {
		return true
	}

	if action == actionDelete && len(c.deleteData) > c.indexDeleteElement {
		return true
	}

	return false
}

func (c *CDCIterator) filterData(data []map[string]interface{}) {
	c.insertData = make([]map[string]interface{}, 0)
	c.updateData = make([]map[string]interface{}, 0)
	c.deleteData = make([]map[string]interface{}, 0)

	for _, v := range data {
		if v[repository.MetadataColumnAction] == "INSERT" && v[repository.MetadataColumnUpdate] == false {
			c.insertData = append(c.insertData, v)
		}

		if v[repository.MetadataColumnAction] == "INSERT" && v[repository.MetadataColumnUpdate] == true {
			c.updateData = append(c.updateData, v)
		}

		if v[repository.MetadataColumnAction] == "DELETE" && v[repository.MetadataColumnUpdate] == false {
			c.deleteData = append(c.deleteData, v)
		}
	}
}

func (c *CDCIterator) hasData() bool {
	for _, v := range actionList {
		if c.hasActionData(v) {
			c.action = v

			return true
		}
	}

	return false
}

// Stop shutdown iterator.
func (c *CDCIterator) Stop() error {
	return c.snowflake.Close()
}

// Ack check if record with position was recorded.
func (c *CDCIterator) Ack(rp sdk.Position) error {
	return nil
}

func getStreamName(table string) string {
	return fmt.Sprintf("%s_%s", Conduit, table)
}
