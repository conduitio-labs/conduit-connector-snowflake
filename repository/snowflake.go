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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/maps"

	sf "github.com/snowflakedb/gosnowflake"

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
	_, err := s.conn.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateStream, stream, table)))

	return err
}

// CreateTrackingTable create stream.
func (s *Snowflake) CreateTrackingTable(ctx context.Context, trackingTable, table string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	_, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTable, trackingTable, table)))
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryAddStringColumn, trackingTable, MetadataColumnAction)))
	if err != nil {
		return fmt.Errorf("add metadata action column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryAddStringColumn, trackingTable, MetadataColumnUpdate)))
	if err != nil {
		return fmt.Errorf("add metadata update column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryAddStringColumn, trackingTable, MetadataColumnRow)))
	if err != nil {
		return fmt.Errorf("add metadata row column: %w", err)
	}

	_, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryAddStringColumn, trackingTable, MetadataColumnTime)))
	if err != nil {
		return fmt.Errorf("add metadata timestamp column: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return err
}

// creates the internal stage,
func (s *Snowflake) SetupStage(ctx context.Context, stage string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateStage, stage))); err != nil {
		return fmt.Errorf("create stage table: %w", err)
	}

	return tx.Commit()

}

// creates temporary, and destination table if they don't exist already.
func (s *Snowflake) SetupTables(ctx context.Context, tableName, batchUUID string, schema map[string]string) (string, error) {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	tempTable := fmt.Sprintf("%s_temp_%s", tableName, batchUUID)
	columnsSQL := buildSchema(schema)

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTemporaryTable, tempTable, columnsSQL))); err != nil {
		return "", fmt.Errorf("create temporary table: %w", err)
	}

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTable, tableName, columnsSQL))); err != nil {
		return "", fmt.Errorf("create destination table: %w", err)
	}

	return tempTable, tx.Commit()
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

	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// Consume data.
	_, err = tx.ExecContext(ctx, buildConsumeDataQuery(trackingTable, stream, fields))
	if err != nil {
		return nil, fmt.Errorf("consume data: %w", err)
	}

	rows, err := tx.QueryContext(ctx, buildGetTrackingData(trackingTable, fields, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	result := make([]map[string]interface{}, 0)

	colValues := make([]interface{}, len(columns))

	for rows.Next() {
		row := make(map[string]interface{}, len(columns))

		for i := range colValues {
			colValues[i] = new(interface{})
		}

		if er := rows.Scan(colValues...); er != nil {
			return nil, fmt.Errorf("scan: %w", err)
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

// TableExists check if table exist.
func (s *Snowflake) TableExists(ctx context.Context, table string) (bool, error) {
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf(queryIsTableExist, strings.ToUpper(table)))
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

func (s *Snowflake) PutFileInStage(ctx context.Context, buf *bytes.Buffer, fileName, stage string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint
	
	if _, err = tx.ExecContext(sf.WithFileStream(ctx, buf), buildQuery(ctx, fmt.Sprintf(queryPutFileInStage, fileName, stage))); err != nil {
		return fmt.Errorf("PUT file %s in stage %s: %w", fileName, stage, err)
	}

	return tx.Commit()
}

func (s *Snowflake) Copy(ctx context.Context, tempTable, stage, fileName string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCopyInto, tempTable, stage, fileName))); err != nil {
		return fmt.Errorf("failed to copy file %s in temp %s: %w", fileName, tempTable, err)
	}

	return tx.Commit()
}

func (s *Snowflake) Merge(ctx context.Context, table, tempTable, prefix string, schema map[string]string, orderingCols []string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	if _, err = tx.ExecContext(ctx, buildMergeQuery(ctx, table, tempTable, prefix, schema, orderingCols)); err != nil {
		return fmt.Errorf("failed to merge into table %s from %s: %w", table, tempTable, err)
	}

	return tx.Commit()
}
func (s *Snowflake) Cleanup(ctx context.Context, stage, fileName string) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryRemoveFile, stage, fileName))); err != nil {
		return fmt.Errorf("failed to remove  %s from %s: %w", fileName, stage, err)
	}

	return tx.Commit()
}

// GetPrimaryKeys returns all primary keys of the table.
func (s *Snowflake) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	var columns []string

	rows, err := s.conn.QueryxContext(ctx, fmt.Sprintf(queryGetPrimaryKeys, table))
	if err != nil {
		return nil, fmt.Errorf("query get max value: %w", err)
	}
	defer rows.Close()

	dest := make(map[string]any)
	for rows.Next() {
		if err = rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("scan primary key row: %w", err)
		}

		columns = append(columns, dest[columnName].(string))
	}

	return columns, nil
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

func buildQuery(ctx context.Context, query string) string {
	sb := sqlbuilder.Build(query)
	s, _ := sb.Build()

	return s
}

func buildMergeQuery(ctx context.Context, tableName, tempTable, prefix string, schema map[string]string, orderingCols []string) string {
	cols := maps.Keys(schema)

	updateSet := buildOrderingColumnList("a", "b", ",", cols)
	orderingColumnList := buildOrderingColumnList("a", "b", " AND ", orderingCols)
	insertColumnList := buildFinalColumnList("a", ".", cols)
	valuesColumnList := buildFinalColumnList("b", ".", cols)
	//`MERGE INTO %s{tableName} as a USING %s{tempTable} AS b ON %s{orderingColumnList}
	// 	WHEN MATCHED AND b.%s{prefix}_operation = 'update' THEN UPDATE SET %s{updateSet}, a.%s{prefix}_updated_at = CURRENT_TIMESTAMP()
	//     WHEN MATCHED AND b.%s{prefix}_operation = 'delete' THEN UPDATE SET a.%s{prefix}_deleted_at = CURRENT_TIMESTAMP()
	// 	WHEN NOT MATCHED THEN INSERT (%s , a.%s_created_at) VALUES (%s, CURRENT_TIMESTAMP())`
	sb := sqlbuilder.Build(fmt.Sprintf(queryMergeInto,
		tableName,
		tempTable,
		orderingColumnList,
		prefix,
		updateSet,
		prefix,
		prefix,
		prefix,
		insertColumnList,
		prefix,
		valuesColumnList))
	s, _ := sb.Build()
	return s
}

func buildFinalColumnList(table, delimiter string, cols []string) string {
	ret := make([]string, len(cols))
	for i, colName := range cols {
		ret[i] = fmt.Sprintf("%s%s%s", table, delimiter, colName)
	}
	return toStr(ret)
}

func buildSchema(schema map[string]string) string {
	cols := make([]string, len(schema))
	i := 0
	for colName, sqlType := range schema {
		cols[i] = fmt.Sprintf("%s %s", colName, sqlType)
		i++
	}
	return toStr(cols)
}

func buildOrderingColumnList(tableFrom, tableTo, delimiter string, orderingCols []string) string {
	ret := make([]string, len(orderingCols))
	for i, colName := range orderingCols {
		ret[i] = fmt.Sprintf("%s.%s = %s.%s", tableFrom, colName, tableTo, colName)
	}
	return strings.Join(ret, delimiter)
}

func isNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
