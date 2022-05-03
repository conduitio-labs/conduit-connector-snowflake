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

	"github.com/huandu/go-sqlbuilder"

	_ "github.com/snowflakedb/gosnowflake" //nolint:revive,nolintlint
)

// Snowflake repository.
type Snowflake struct {
	conn *sql.Conn
}

// Create storage.
func Create(ctx context.Context, connectionData string) (Snowflake, error) {
	db, err := sql.Open("snowflake", connectionData)
	if err != nil {
		return Snowflake{}, fmt.Errorf("open db: %v", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return Snowflake{}, fmt.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return Snowflake{}, fmt.Errorf("create conn: %v", err)
	}

	return Snowflake{conn: conn}, nil
}

// Close conn.
func (s Snowflake) Close() error {
	return s.conn.Close()
}

// GetData get rows with columns offset from table.
func (s Snowflake) GetData(
	ctx context.Context,
	table string,
	fields []string,
	offset, limit int,
) ([]map[string]interface{}, error) {
	rows, err := s.conn.QueryContext(ctx, buildQuery(table, fields, offset, limit))
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

func buildQuery(table string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()

	if fields == nil {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.Offset(offset)
	sb.Limit(limit)

	return sb.String()
}
