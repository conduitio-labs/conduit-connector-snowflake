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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	ErrMissingSQLState = "42S02"
	ErrMissingCode     = 2003
)

type snowflkaeTableDef struct {
	name          string
	typ           string
	kind          string
	isNull        string
	defaultVal    *string
	pkey          string
	uniqKey       string
	check         *string
	exp           *string
	comment       *string
	policyName    *string
	privacyDomain *string
	// schemaEvolution *string - only visible when schema evolution is enabled
}

var snowflakeTypes = map[reflect.Kind]string{
	reflect.Int:     "bigint",
	reflect.Int8:    "bigint",
	reflect.Int16:   "bigint",
	reflect.Int32:   "bigint",
	reflect.Int64:   "bigint",
	reflect.String:  "varchar",
	reflect.Bool:    "boolean",
	reflect.Float32: "double",
	reflect.Float64: "double",
}

type Evolver struct {
	db  *sql.DB
	buf bytes.Buffer
}

func NewEvolver(db *sql.DB) *Evolver {
	return &Evolver{db: db}
}

// Migrate evolves the snowflake table to match that of the provided schema.
func (e *Evolver) Migrate(ctx context.Context, table string, sch Schema) (bool, error) {
	tableFields := make(map[string]string)

	// find all fields of the current table
	rows, err := e.db.QueryContext(
		ctx,
		fmt.Sprintf("DESC TABLE %s", table),
	)
	if err != nil {
		if notFound(err) {
			return e.migrateTable(ctx, table, sch, tableFields) // table not found make it
		}

		return false, errors.Errorf("failed to describe table %q: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var s snowflkaeTableDef

		if err := rows.Scan(
			&s.name, &s.typ, &s.kind, &s.isNull,
			&s.defaultVal, &s.pkey, &s.uniqKey,
			&s.check, &s.exp, &s.comment, &s.policyName,
			&s.privacyDomain, // &s.schemaEvolution,
		); err != nil {
			return false, errors.Errorf("failed to scan rows: %w", err)
		}

		tableFields[strings.ToLower(s.name)] = strings.ToLower(s.typ)
	}

	if err := rows.Err(); err != nil {
		return false, errors.Errorf("failed to iterate over rows result: %w", err)
	}

	return e.migrateTable(ctx, table, sch, tableFields)
}

func (e *Evolver) migrateTable(ctx context.Context, table string, sch Schema, fields map[string]string) (bool, error) {
	start := time.Now()

	var newColumns []string

	for k, v := range sch {
		if _, ok := fields[k]; ok {
			continue // exists, ignore type
		}

		snowflakeType, ok := snowflakeTypes[v]
		if !ok {
			return false, errors.Errorf("cannot find match type %q in snowflake", v)
		}

		newColumns = append(
			newColumns,
			fmt.Sprintf("%s %s", k, snowflakeType),
		)
	}

	if len(newColumns) == 0 {
		sdk.Logger(ctx).Debug().Msg("evolver: no new columns to add")

		return false, nil
	}

	fmt.Fprintf(&e.buf, "ALTER TABLE %s ADD COLUMN %s", table, strings.Join(newColumns, ", "))

	sdk.Logger(ctx).Debug().
		Str("alter", e.buf.String()).
		Msgf("adding new columns to table %q", table)

	if _, err := e.db.ExecContext(ctx, e.buf.String()); err != nil {
		return false, errors.Errorf("failed to evolve schema on table %q: %w", table, err)
	}

	sdk.Logger(ctx).Debug().Dur("duration", time.Since(start)).Msgf("finished migrating schema")

	return true, nil
}

func notFound(err error) bool {
	if nErr := err.(*sf.SnowflakeError); nErr != nil {
		return nErr.Number == ErrMissingCode && nErr.SQLState == ErrMissingSQLState
	}

	return false
}
