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

	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

// Repository interface.
type Repository interface {
	// GetRows - get rows from table.
	GetRows(ctx context.Context, table, orderingColumn string, fields []string, pos *position.Position, maxValue any,
		limit int) (*sqlx.Rows, error)
	// CreateStream - create stream.
	CreateStream(ctx context.Context, stream, table string) error
	// GetTrackingData - get rows from tracking table.
	GetTrackingData(ctx context.Context, stream, trackingTable string, fields []string,
		offset, limit int,
	) ([]map[string]interface{}, error)
	// CreateTrackingTable - create tracking table.
	CreateTrackingTable(ctx context.Context, trackingTable, table string) error
	// GetMaxValue get max value by ordering column.
	GetMaxValue(ctx context.Context, table, orderingColumn string) (any, error)
	// Close - shutdown repository.
	Close() error
	// GetPrimaryKeys returns all primary keys of the table.
	GetPrimaryKeys(ctx context.Context, table string) ([]string, error)
}
