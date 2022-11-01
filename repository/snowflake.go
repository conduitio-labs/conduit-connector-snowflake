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

package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"

	_ "github.com/snowflakedb/gosnowflake" //nolint:revive,nolintlint

	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

var (
	MetadataFields = []string{MetadataColumnAction, MetadataColumnUpdate, MetadataColumnTime}
)

// Snowflake repository.
type Snowflake struct {
	conn *sqlx.Conn
}

// Create storage.
func Create(ctx context.Context, connectionData string) (*Snowflake, error) {
	db, err := sqlx.Open("snowflake", connectionData)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	conn, err := db.Connx(ctx)
	if err != nil {
		return nil, fmt.Errorf("create conn: %w", err)
	}

	return &Snowflake{conn: conn}, nil
}

// Close conn.
func (s *Snowflake) Close() error {
	return s.conn.Close()
}

// GetRows get rows with columns offset from table.
func (s *Snowflake) GetRows(
	ctx context.Context,
	table, orderingColumn string,
	fields []string,
	pos *position.Position,
	maxValue any,
	limit int,
) (*sqlx.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	sb := sqlbuilder.NewSelectBuilder()

	if len(fields) == 0 {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.OrderBy(orderingColumn)
	if pos != nil {
		sb.Where(
			sb.GreaterThan(orderingColumn, pos.SnapshotLastProcessedVal),
		)
	}
	if !isNil(maxValue) {
		sb.Where(sb.LessEqualThan(orderingColumn, maxValue))
	}
	sb.Limit(limit)

	query, args := sb.Build()

	rows, err := s.conn.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}

	return rows, nil
}

// CreateStream create stream.
func (s *Snowflake) CreateStream(ctx context.Context, stream, table string) error {
	_, err := s.conn.ExecContext(ctx, buildCreateStreamQuery(stream, table))

	return err
}

// CreateTrackingTable create stream.
func (s *Snowflake) CreateTrackingTable(ctx context.Context, trackingTable, table string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	_, err = tx.ExecContext(ctx, buildCreateTrackingTable(trackingTable, table))
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildAddStringColumn(trackingTable, MetadataColumnAction))
	if err != nil {
		return fmt.Errorf("add metadata action column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildAddBoolColumn(trackingTable, MetadataColumnUpdate))
	if err != nil {
		return fmt.Errorf("add metadata update column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildAddStringColumn(trackingTable, MetadataColumnRow))
	if err != nil {
		return fmt.Errorf("add metadata row column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildAddTimestampColumn(trackingTable, MetadataColumnTime))
	if err != nil {
		return fmt.Errorf("add metadata timestamp column: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return err
}

// GetTrackingData get data from tracking table.
func (s *Snowflake) GetTrackingData(
	ctx context.Context,
	stream, trackingTable string,
	fields []string,
	limit int,
) (*sqlx.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Consume data.
	_, err := s.conn.ExecContext(ctx, buildConsumeDataQuery(trackingTable, stream, fields))
	if err != nil {
		return nil, fmt.Errorf("consume data: %w", err)
	}

	rows, err := s.conn.QueryxContext(ctx, buildGetTrackingDataQuery(trackingTable, fields, limit))
	if err != nil {
		return nil, fmt.Errorf("get tracking data %w", err)
	}

	return rows, nil
}

// IsTableExist check if table exist.
func (s *Snowflake) IsTableExist(ctx context.Context, table string) (bool, error) {
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf(queryIsTableExist, strings.ToUpper(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

// IsStreamExist check if stream exist.
func (s *Snowflake) IsStreamExist(ctx context.Context, stream string) (bool, error) {
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf(queryIsStreamExist, strings.ToUpper(stream)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

// GetMaxValue get max value by ordering column.
func (s *Snowflake) GetMaxValue(ctx context.Context, table, orderingColumn string) (any, error) {
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf(queryGetMaxValue, orderingColumn, table))
	if err != nil {
		return nil, fmt.Errorf("query get max value: %w", err)
	}

	defer rows.Close()

	var maxValue any
	for rows.Next() {
		er := rows.Scan(&maxValue)
		if er != nil {
			return nil, er
		}
	}

	return maxValue, nil
}

// DeleteTrackingData - delete rows from tracking table.
func (s *Snowflake) DeleteTrackingData(ctx context.Context, table string, ids []any) error {
	if len(ids) == 0 {
		return nil
	}

	db := sqlbuilder.NewDeleteBuilder()
	db.DeleteFrom(table)
	db.Where(db.In(MetadataColumnRow, ids...))

	query, args := db.Build()

	_, err := s.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete rows: %w", err)
	}

	return nil
}

func buildGetTrackingDataQuery(table string, fields []string, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()
	if len(fields) == 0 {
		sb.Select("*")
	} else {
		fields = append(fields, MetadataFields...)
		sb.Select(fields...)
	}

	sb.From(table)
	sb.OrderBy(MetadataColumnTime)
	sb.Limit(limit)

	return sb.String()
}

func buildConsumeDataQuery(trackingTable, stream string, fields []string) string {
	selectSb := sqlbuilder.NewSelectBuilder()
	if len(fields) == 0 {
		selectSb.Select("*, current_timestamp()")
	} else {
		columns := fields
		columns = append(columns, MetadataColumnAction)
		columns = append(columns, MetadataColumnUpdate)
		columns = append(columns, "current_timestamp()")
		selectSb.Select(columns...)
	}

	selectSb.From(stream)

	var sb sqlbuilder.Builder
	if fields == nil {
		sb = sqlbuilder.Build(fmt.Sprintf(queryInsertInto, trackingTable, selectSb.String()))
	} else {
		columns := fields
		columns = append(columns, MetadataFields...)
		sb = sqlbuilder.Build(fmt.Sprintf(queryInsertIntoColumn, trackingTable, toStr(columns), selectSb.String()))
	}
	s, _ := sb.Build()

	return s
}

func toStr(fields []string) string {
	return strings.Join(fields, ", ")
}

func buildCreateStreamQuery(stream, table string) string {
	sb := sqlbuilder.Build(fmt.Sprintf(queryCreateStream, stream, table))
	s, _ := sb.Build()

	return s
}

func buildCreateTrackingTable(trackingTable, table string) string {
	sb := sqlbuilder.Build(fmt.Sprintf(queryCreateTrackingTable, trackingTable, table))
	s, _ := sb.Build()

	return s
}

func buildAddStringColumn(table, column string) string {
	sb := sqlbuilder.Build(fmt.Sprintf(queryAddStringColumn, table, column))
	s, _ := sb.Build()

	return s
}

func buildAddBoolColumn(table, column string) string {
	sb := sqlbuilder.Build(fmt.Sprintf(queryAddBooleanColumn, table, column))
	s, _ := sb.Build()

	return s
}

func buildAddTimestampColumn(table, column string) string {
	sb := sqlbuilder.Build(fmt.Sprintf(queryAddTimestampColumn, table, column))
	s, _ := sb.Build()

	return s
}

func isNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
