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

package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
	tableKeys map[string]string
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Source Connector.")

	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("failed to parse source config : %w", err)
	}

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	var err error
	if s.readingAllTables() {
		sdk.Logger(ctx).Info().Msg("Detecting all tables...")
		s.config.Tables, err = s.getAllTables(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to get all tables: %w", err)
		}
		sdk.Logger(ctx).Info().
			Strs("tables", s.config.Tables).
			Int("count", len(s.config.Tables)).
			Msg("Successfully detected tables")
	}

	// ensure we have keys for all tables
	for _, tableName := range s.config.Tables {
		s.tableKeys[tableName], err = s.getTableKeys(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to find key for table %s (try specifying it manually): %w", tableName, err)
		}
	}


	it, err := iterator.New(ctx, s.config.Connection, s.config.Table, s.config.OrderingColumn, s.config.Keys,
		s.config.Columns, s.config.BatchSize, s.config.Snapshot, rp)
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}

	s.iterator = it

	return nil
}

// Read gets the next object from the snowflake.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p sdk.Position) error {
	return s.iterator.Ack(ctx, p)
}

func (s *Source) readingAllTables() bool {
	return len(s.config.Tables) == 1 && s.config.Tables[0] == AllTablesWildcard
}

func (s *Source) getAllTables(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES in SCHEMA %s.%s;", s.config.Database, s.config.Schema)

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return tables, nil
}

// getTableKeys queries the db for the name of the primary key column for a
// table if one exists and returns it.
func (s *Source) getTableKeys(ctx context.Context, tableName string) (string, error) {
	// TODO: improve the query. unfortunately this is the recommended approach by snowflake:
	// https://community.snowflake.com/s/article/Columns-associated-with-a-primary-key
	query := fmt.Sprintf(`SHOW PRIMARY KEYS in TABLE %s;
	SELECT "table_name","column_name"
	FROM TABLE(result_scan(last_query_id()))
	ORDER BY "table_name";`, tableName)

	rows, err := s.pool.Query(ctx, query, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to query table keys: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Err() != nil {
			return "", fmt.Errorf("query failed: %w", rows.Err())
		}
		return "", errors.New("no table keys found")
	}

	var colName string
	err = rows.Scan(&colName)
	if err != nil {
		return "", fmt.Errorf("failed to scan row: %w", err)
	}

	if rows.Next() {
		// we only support single column primary keys for now
		return "", errors.New("composite keys are not supported")
	}

	return colName, nil
}
