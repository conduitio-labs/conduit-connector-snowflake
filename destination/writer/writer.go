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

package writer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/google/uuid"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

// Writer is an interface that is responsible for persisting record that Destination
// has accumulated in its buffers. The default writer the Destination would use is
// SnowflakeWriter, others exists to test local behavior.
type Writer interface {
	Write(context.Context, []sdk.Record) (int, error)
	Close(context.Context) error
}

// Snowflake writer stores batch bytes into an Snowflake bucket as a file.
type Snowflake struct {
	Prefix      string
	PrimaryKey  []string
	Stage       string
	TableName   string
	FileThreads int

	db         *sql.DB
	insertsBuf *bytes.Buffer
	updatesBuf *bytes.Buffer
}

var _ Writer = (*Snowflake)(nil)

// SnowflakeConfig is a type used to initialize an Snowflake Writer.
type SnowflakeConfig struct {
	Prefix      string
	PrimaryKey  []string
	Stage       string
	TableName   string
	Connection  string
	FileThreads int
}

// NewSnowflake takes an SnowflakeConfig reference and produces an Snowflake Writer.
func NewSnowflake(ctx context.Context, cfg *SnowflakeConfig) (*Snowflake, error) {
	db, err := sql.Open("snowflake", cfg.Connection)
	if err != nil {
		return nil, errors.Errorf("failed to connect to snowflake db")
	}

	// create the stage if it doesn't exist, replace it if already present
	createStageQuery := fmt.Sprintf("CREATE OR REPLACE STAGE %s", cfg.Stage)
	sdk.Logger(ctx).Debug().Msgf("executing: %s", createStageQuery)
	if _, err := db.ExecContext(
		ctx,
		createStageQuery,
	); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to create stage")

		return nil, errors.Errorf("failed to create stage %q: %w", cfg.Stage, err)
	}

	return &Snowflake{
		Prefix:      cfg.Prefix,
		PrimaryKey:  cfg.PrimaryKey,
		Stage:       cfg.Stage,
		TableName:   cfg.TableName,
		FileThreads: cfg.FileThreads,
		db:          db,
		insertsBuf:  &bytes.Buffer{},
		updatesBuf:  &bytes.Buffer{},
	}, nil
}

func (s *Snowflake) Close(ctx context.Context) error {
	dropStageQuery := fmt.Sprintf("DROP STAGE %s", s.Stage)
	sdk.Logger(ctx).Debug().Msgf("executing: %s", dropStageQuery)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("DROP STAGE %s", s.Stage)); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to gracefully close the connection")

		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	if err := s.db.Close(); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to gracefully close the connection")

		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

func (s *Snowflake) Write(ctx context.Context, records []sdk.Record) (int, error) {
	var (
		indexCols []string
		colOrder  []string
		err       error
	)

	schema := make(map[string]string)

	indexCols, colOrder, err = format.MakeCSVBytes(
		records,
		schema,
		s.Prefix,
		s.PrimaryKey,
		s.insertsBuf,
		s.updatesBuf,
	)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to convert records to CSV")

		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// Clean out the buffers after write. Storage will be reused next.
	defer s.insertsBuf.Reset()
	defer s.updatesBuf.Reset()

	// generate a UUID used for the temporary table and filename in internal stage
	batchUUID := strings.ReplaceAll(uuid.NewString(), "-", "")
	var insertsFilename, updatesFilename string

	tempTable, err := s.SetupTables(ctx, batchUUID, schema)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to set up snowflake tables")

		return 0, errors.Errorf("failed to set up snowflake tables: %w", err)
	}

	sdk.Logger(ctx).Debug().Msg("set up necessary tables")
	sdk.Logger(ctx).Debug().Msgf("insertBuffer.Len()=%d, updatesBuf.Len()=%d", s.insertsBuf.Len(), s.updatesBuf.Len())

	if s.insertsBuf != nil && s.insertsBuf.Len() > 0 {
		insertsFilename = fmt.Sprintf("inserts_%s.csv.gz", batchUUID)
		if err := s.PutFileInStage(ctx, s.insertsBuf, insertsFilename); err != nil {
			sdk.Logger(ctx).Err(err).Msg("failed put CSV file to snowflake stage")

			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
	}

	if s.updatesBuf != nil && s.updatesBuf.Len() > 0 {
		updatesFilename = fmt.Sprintf("updates_%s.csv.gz", batchUUID)
		if err := s.PutFileInStage(ctx, s.updatesBuf, updatesFilename); err != nil {
			sdk.Logger(ctx).Err(err).Msg("failed put CSV file to snowflake stage")

			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
	}

	if err := s.CopyAndMerge(ctx, tempTable, insertsFilename, updatesFilename, colOrder, indexCols, schema); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to process records")

		return 0, errors.Errorf("failed to process records: %w", err)
	}

	return len(records), nil
}

// creates temporary, and destination table if they don't exist already.
func (s *Snowflake) SetupTables(ctx context.Context, batchUUID string, schema map[string]string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to begin transaction")

		return "", errors.Errorf("failed to create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	tempTable := fmt.Sprintf("%s_temp_%s", s.TableName, batchUUID)
	columnsSQL := buildSchema(schema)
	pks := strings.Join(s.PrimaryKey, ", ")
	queryCreateTempTable := fmt.Sprintf(
		`CREATE TEMPORARY TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s))`,
		tempTable, columnsSQL, pks)
	sdk.Logger(ctx).Debug().Msgf("executing: %s", queryCreateTempTable)
	if _, err = tx.ExecContext(ctx, queryCreateTempTable); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to create temporary table")

		return "", errors.Errorf("failed to create temporary table: %w", err)
	}

	queryCreateTable := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		%s,
		meroxa_deleted_at TIMESTAMP_LTZ,
		meroxa_created_at TIMESTAMP_LTZ,
		meroxa_updated_at TIMESTAMP_LTZ,
		PRIMARY KEY (%s)
	)`, s.TableName, columnsSQL, pks)

	sdk.Logger(ctx).Debug().Msgf("executing: %s", queryCreateTable)

	if _, err = tx.ExecContext(ctx, queryCreateTable); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to create destination table")

		return "", errors.Errorf("failed to create destination table: %w", err)
	}

	return tempTable, tx.Commit()
}

func (s *Snowflake) PutFileInStage(ctx context.Context, buf *bytes.Buffer, filename string) error {
	// nolint:errcheck,nolintlint
	putQuery := fmt.Sprintf(
		"PUT file://%s @%s SOURCE_COMPRESSION=GZIP parallel=%d;",
		filename,
		s.Stage,
		s.FileThreads,
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to begin tx")

		return errors.Errorf("failed to begin tx: %w", err)
	}

	ctxFs := sf.WithFileStream(ctx, buf)
	ctxFs = sf.WithFileTransferOptions(ctxFs, &sf.SnowflakeFileTransferOptions{
		RaisePutGetError: true,
	})

	sdk.Logger(ctx).Debug().Msgf("executing: %s", putQuery)

	if _, err := tx.ExecContext(ctxFs, putQuery); err != nil {
		sdk.Logger(ctx).Err(err).Msgf("PUT file %s in stage %s", filename, s.Stage)

		return errors.Errorf("PUT file %s in stage %s: %w", filename, s.Stage, err)
	}

	if err := tx.Commit(); err != nil {
		sdk.Logger(ctx).Err(err).Msg("error putting file in stage")

		return errors.Errorf("error putting file in stage: %w", err)
	}

	return nil
}

func (s *Snowflake) CopyAndMerge(ctx context.Context, tempTable, insertsFilename, updatesFilename string,
	colOrder, indexCols []string, schema map[string]string,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Info().Msg("start of copyandmerge")

	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			sdk.Logger(ctx).Err(err).Msg("rolling back transaction")
		}
	}()

	if insertsFilename != "" {
		// COPY INTO for inserts
		sdk.Logger(ctx).Debug().Msg("constructing query for COPY INTO for inserts")
		colList := buildFinalColumnList("", "", colOrder)
		aliasFields := make([]string, len(colOrder))
		for i := 0; i < len(colOrder); i++ {
			aliasFields[i] = fmt.Sprintf(`f.$%d`, i+1)
		}
		aliasCols := strings.Join(aliasFields, ", ")
		//nolint:gosec // use proper SQL statement preparation
		copyIntoQuery := fmt.Sprintf(
			`COPY INTO %s (%s, %s_created_at)
			FROM (
				SELECT %s, CURRENT_TIMESTAMP()
				FROM @%s/%s f
			)
			FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);`,
			s.TableName, colList, s.Prefix, aliasCols, s.Stage, insertsFilename,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", copyIntoQuery)

		if _, err = tx.ExecContext(ctx, copyIntoQuery); err != nil {
			sdk.Logger(ctx).Err(err).Msgf("failed to copy file %s in %s", insertsFilename, s.TableName)

			return errors.Errorf("failed to copy file %s in %s: %w", insertsFilename, s.TableName, err)
		}
		sdk.Logger(ctx).Info().Msg("ran COPY INTO for inserts")
	}

	if updatesFilename != "" {
		// COPY INTO for updates
		//nolint:gosec // use proper SQL statement preparation
		copyIntoQuery := fmt.Sprintf(`
			COPY INTO %s FROM @%s
			FILES = ('%s.gz')
			FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  PARSE_HEADER = TRUE)
			MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE' PURGE = TRUE;`,
			tempTable,
			s.Stage,
			updatesFilename,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", copyIntoQuery)

		if _, err = tx.ExecContext(ctx, copyIntoQuery); err != nil {
			sdk.Logger(ctx).Err(err).Msgf("failed to copy file %s in temp %s", updatesFilename, tempTable)

			return errors.Errorf("failed to copy file %s in temp %s: %w", updatesFilename, tempTable, err)
		}

		sdk.Logger(ctx).Info().Msg("ran COPY INTO for updates/deletes")

		// MERGE
		cols := maps.Keys(schema)
		updateSet := buildOrderingColumnList("a", "b", ",", cols)
		orderingColumnList := buildOrderingColumnList("a", "b", " AND ", indexCols)

		//nolint:gosec // not an issue
		queryMergeInto := fmt.Sprintf(
			`MERGE INTO %s as a USING %s AS b ON %s
			WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s, a.%s_updated_at = CURRENT_TIMESTAMP()
			WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET a.%s_deleted_at = CURRENT_TIMESTAMP()`,
			s.TableName,
			tempTable,
			orderingColumnList,
			s.Prefix,
			updateSet,
			s.Prefix,
			s.Prefix,
			s.Prefix,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", queryMergeInto)

		if _, err = tx.ExecContext(ctx, queryMergeInto); err != nil {
			sdk.Logger(ctx).Err(err).Msgf("failed to merge into table %s from %s", s.TableName, tempTable)

			return errors.Errorf("failed to merge into table %s from %s: %w", s.TableName, tempTable, err)
		}

		sdk.Logger(ctx).Info().Msg("ran MERGE for updates/deletes")
	}

	if err := tx.Commit(); err != nil {
		sdk.Logger(ctx).Err(err).Msg("transaction failed")

		return errors.Errorf("transaction failed: %w", err)
	}

	return nil
}

func buildFinalColumnList(table, delimiter string, cols []string) string {
	ret := make([]string, len(cols))
	for i, colName := range cols {
		ret[i] = strings.Join([]string{table, colName}, delimiter)
	}

	return strings.Join(ret, ", ")
}

func buildSchema(schema map[string]string) string {
	cols := make([]string, len(schema))
	i := 0
	for colName, sqlType := range schema {
		cols[i] = strings.Join([]string{colName, sqlType}, " ")
		i++
	}

	return strings.Join(cols, ", ")
}

func buildOrderingColumnList(tableFrom, tableTo, delimiter string, orderingCols []string) string {
	ret := make([]string, len(orderingCols))
	for i, colName := range orderingCols {
		ret[i] = fmt.Sprintf("%s.%s = %s.%s", tableFrom, colName, tableTo, colName)
	}

	return strings.Join(ret, delimiter)
}
