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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/huandu/go-sqlbuilder"

	_ "github.com/snowflakedb/gosnowflake" //nolint:revive,nolintlint
)

var MetadataFields = []string{MetadataColumnAction, MetadataColumnUpdate, MetadataColumnTime}

const queryTimeout = 10

// Snowflake repository.
type Snowflake struct {
	conn *sql.Conn
}

// Create storage.
func Create(ctx context.Context, connectionData string) (*Snowflake, error) {
	db, err := sql.Open("snowflake", connectionData)
	if err != nil {
		return nil, fmt.Errorf("open db: %v", err)
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("create conn: %v", err)
	}

	return &Snowflake{conn: conn}, nil
}

// Close conn.
func (s *Snowflake) Close() error {
	return s.conn.Close()
}

// GetData get rows with columns offset from table.
func (s *Snowflake) GetData(
	ctx context.Context,
	table, key string,
	fields []string,
	offset, limit int,
) ([]map[string]interface{}, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ctxTime, cancel := context.WithTimeout(context.Background(), queryTimeout*time.Second)
	defer cancel()

	rows, err := s.conn.QueryContext(ctxTime, buildGetDataQuery(table, key, fields, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("run query: %v", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %v", err)
	}

	result := make([]map[string]interface{}, 0)

	colValues := make([]interface{}, len(columns))

	for rows.Next() {
		row := make(map[string]interface{}, len(columns))

		for i := range colValues {
			colValues[i] = new(interface{})
		}

		if er := rows.Scan(colValues...); er != nil {
			return nil, fmt.Errorf("scan: %v", err)
		}

		for i, col := range columns {
			row[col] = *colValues[i].(*interface{})
		}

		result = append(result, row)
	}

	return result, nil
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
	offset, limit int,
) ([]map[string]interface{}, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ctxTime, cancel := context.WithTimeout(context.Background(), queryTimeout*time.Second)
	defer cancel()

	tx, err := s.conn.BeginTx(ctxTime, nil)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// Consume data.
	_, err = tx.ExecContext(ctxTime, buildConsumeDataQuery(trackingTable, stream, fields))
	if err != nil {
		return nil, fmt.Errorf("consume data: %v", err)
	}

	rows, err := tx.QueryContext(ctxTime, buildGetTrackingData(trackingTable, fields, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("run query: %v", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %v", err)
	}

	result := make([]map[string]interface{}, 0)

	colValues := make([]interface{}, len(columns))

	for rows.Next() {
		row := make(map[string]interface{}, len(columns))

		for i := range colValues {
			colValues[i] = new(interface{})
		}

		if er := rows.Scan(colValues...); er != nil {
			return nil, fmt.Errorf("scan: %v", err)
		}

		for i, col := range columns {
			row[col] = *colValues[i].(*interface{})
		}

		result = append(result, row)
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return result, nil
}

func buildGetDataQuery(table, key string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()

	if len(fields) == 0 {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.Offset(offset)
	sb.Limit(limit)

	return sb.String()
}

func buildGetTrackingData(table string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()
	if len(fields) == 0 {
		sb.Select("*")
	} else {
		fields = append(fields, MetadataFields...)
		sb.Select(fields...)
	}

	sb.From(table)
	sb.OrderBy(MetadataColumnTime)
	sb.Offset(offset)
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
