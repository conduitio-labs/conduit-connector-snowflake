// Copyright © 2024 Meroxa, Inc.
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

package schema

import (
	"context"
	"database/sql"

	"github.com/go-errors/errors"
)

var ErrTableNotFound = errors.New("table not found")

type tableParser struct {
	db *sql.DB
}

func newTableParser(db *sql.DB) *tableParser {
	return &tableParser{db: db}
}

// Parse reads the table schema and generates a local representation.
func (t *tableParser) Parse(ctx context.Context, table string) (Schema, error) {
	if err := t.exists(ctx, table); err != nil {
		return nil, err
	}

	rows, err := t.db.QueryContext(
		ctx,
		`SELECT
			column_name,
			data_type,
			numeric_precision,
			numeric_scale,
			character_maximum_length
		 FROM information_schema.columns WHERE table_name = ?`,
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sch := make(Schema)

	for rows.Next() {
		var (
			column, dataType         string
			precision, scale, varlen sql.NullInt64
		)

		if err := rows.Scan(
			&column,
			&dataType,
			&precision,
			&scale,
			&varlen,
		); err != nil {
			return nil, errors.Errorf("failed to scan row: %w", err)
		}

		typ, err  := ToType(dataType)
		if err != nil {
			return nil, errors.Errorf("failed to find data type for column %q: %w", column, err)
		}

		c := Column{
			Name:      column,
			Type:      typ,
			Precision: -1,
			Scale: -1,
			Varlen: -1,
		}

		if precision.Valid {
			c.Precision = precision.Int64
		}

		if scale.Valid {
			c.Scale = scale.Int64
		}

		if varlen.Valid {
			c.Varlen = varlen.Int64
		}

		sch[column] = c
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sch, nil
}

func (t *tableParser) exists(ctx context.Context, table string) error {
	var exists bool

	if err := t.db.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = ?)",
		table,
	).Scan(&exists); err != nil {
		return err
	}

	if !exists {
		return ErrTableNotFound
	}

	return nil
}
