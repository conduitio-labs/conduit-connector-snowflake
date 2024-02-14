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
)

// Writer is an interface that is responsible for persisting record that Destination
// has accumulated in its buffers. The default writer the Destination would use is
// SnowflakeWriter, others exists to test local behavior.
type Writer interface {
	Write(context.Context, []sdk.Record) (int, error)
	Close(context.Context) error
}

// SnowflakeCSV writer stores batch bytes into an SnowflakeCSV bucket as a file.
type SnowflakeCSV struct {
	Prefix        string
	PrimaryKey    string
	Stage         string
	TableName     string
	FileThreads   int
	CSVGoroutines int

	db         *sql.DB
	insertsBuf *bytes.Buffer
	updatesBuf *bytes.Buffer
}

type setListMode string

const (
	insertSetMode setListMode = "insert"
	updateSetMode setListMode = "update"
	deleteSetMode setListMode = "delete"
)

var _ Writer = (*SnowflakeCSV)(nil)

// SnowflakeConfig is a type used to initialize an Snowflake Writer.
type SnowflakeConfig struct {
	Prefix        string
	PrimaryKey    string
	Stage         string
	TableName     string
	Connection    string
	CSVGoroutines int
	FileThreads   int
}

// NewCSV takes an SnowflakeConfig reference and produces an SnowflakeCSV Writer.
func NewCSV(ctx context.Context, cfg *SnowflakeConfig) (*SnowflakeCSV, error) {
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

	return &SnowflakeCSV{
		Prefix:        cfg.Prefix,
		PrimaryKey:    cfg.PrimaryKey,
		Stage:         cfg.Stage,
		TableName:     cfg.TableName,
		CSVGoroutines: cfg.CSVGoroutines,
		FileThreads:   cfg.FileThreads,
		db:            db,
		insertsBuf:    &bytes.Buffer{},
		updatesBuf:    &bytes.Buffer{},
	}, nil
}

func (s *SnowflakeCSV) Close(ctx context.Context) error {
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

func (s *SnowflakeCSV) Write(ctx context.Context, records []sdk.Record) (int, error) {
	var (
		colOrder []string
		err      error
	)

	schema := make(map[string]string)

	colOrder, err = format.MakeCSVBytes(
		ctx,
		records,
		schema,
		s.Prefix,
		s.PrimaryKey,
		s.insertsBuf,
		s.updatesBuf,
		s.CSVGoroutines,
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

	err = s.SetupTables(ctx, batchUUID, schema)
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

	if err := s.CopyAndMerge(ctx, insertsFilename, updatesFilename, colOrder, schema); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to process records")

		return 0, errors.Errorf("failed to process records: %w", err)
	}

	return len(records), nil
}

// creates temporary, and destination table if they don't exist already.
func (s *SnowflakeCSV) SetupTables(ctx context.Context, batchUUID string, schema map[string]string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to begin transaction")

		return errors.Errorf("failed to create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	columnsSQL := buildSchema(schema)

	queryCreateTable := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		%s,
		PRIMARY KEY (%s)
	)`, s.TableName, columnsSQL, s.PrimaryKey)

	sdk.Logger(ctx).Debug().Msgf("executing: %s", queryCreateTable)

	if _, err = tx.ExecContext(ctx, queryCreateTable); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to create destination table")

		return errors.Errorf("failed to create destination table: %w", err)
	}

	return tx.Commit()
}

func (s *SnowflakeCSV) PutFileInStage(ctx context.Context, buf *bytes.Buffer, filename string) error {
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

func (s *SnowflakeCSV) CopyAndMerge(ctx context.Context, insertsFilename, updatesFilename string,
	colOrder []string, schema map[string]string,
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

	orderingColumnList := fmt.Sprintf("a.%s = b.%s", s.PrimaryKey, s.PrimaryKey)
	insertSetCols := s.buildSetList("a", "b", colOrder, insertSetMode)
	updateSetCols := s.buildSetList("a", "b", colOrder, updateSetMode)
	deleteSetCols := s.buildSetList("a", "b", colOrder, deleteSetMode)

	colListA := buildFinalColumnList("a", ".", colOrder)
	colListB := buildFinalColumnList("b", ".", colOrder)
	setSelectMerge := buildSelectMerge(colOrder)

	sdk.Logger(ctx).Debug().Msgf("insertsFilename=%s, updatesFilename=%s", insertsFilename, updatesFilename)

	if insertsFilename != "" {
		// COPY INTO for inserts
		sdk.Logger(ctx).Debug().Msg("constructing merge query for inserts")

		//nolint:gosec // not an issue
		queryMergeInto := fmt.Sprintf(
			`MERGE INTO %s as a USING ( select %s from @%s/%s (FILE_FORMAT =>  csv ) )AS b ON %s
			WHEN MATCHED AND ( b.%s_operation = 'create' OR b.%s_operation = 'snapshot' ) THEN UPDATE SET %s
			WHEN NOT MATCHED AND ( b.%s_operation = 'create' OR b.%s_operation = 'snapshot' ) THEN INSERT  (%s) VALUES (%s) ; `,
			s.TableName,
			setSelectMerge,
			s.Stage,
			insertsFilename,
			orderingColumnList,
			// second line
			s.Prefix,
			s.Prefix,
			insertSetCols,
			// third line
			s.Prefix,
			s.Prefix,
			colListA,
			colListB,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", queryMergeInto)

		if _, err = tx.ExecContext(ctx, queryMergeInto); err != nil {
			sdk.Logger(ctx).Err(err).Msgf("failed to merge into table %s from %s", s.TableName, insertsFilename)

			return errors.Errorf("failed to merge into table %s from %s: %w", s.TableName, insertsFilename, err)
		}

		sdk.Logger(ctx).Info().Msg("ran MERGE for inserts")
	}

	if updatesFilename != "" {
		sdk.Logger(ctx).Debug().Msg("constructing merge query for update / delete")

		//nolint:gosec // not an issue
		queryMergeInto := fmt.Sprintf(
			`MERGE INTO %s as a USING ( select %s from @%s/%s (FILE_FORMAT =>  csv ) )AS b ON %s
			WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s
			WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET %s
			WHEN NOT MATCHED AND b.%s_operation = 'update' THEN INSERT  (%s) VALUES (%s)
			WHEN NOT MATCHED AND b.%s_operation = 'delete' THEN INSERT  (%s) VALUES (%s) ; `,
			s.TableName,
			setSelectMerge,
			s.Stage,
			updatesFilename,
			orderingColumnList,
			// second line
			s.Prefix,
			updateSetCols,
			// third line
			s.Prefix,
			deleteSetCols,
			// fourth line
			s.Prefix,
			colListA,
			colListB,
			// fifth line
			s.Prefix,
			colListA,
			colListB,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", queryMergeInto)

		if _, err = tx.ExecContext(ctx, queryMergeInto); err != nil {
			sdk.Logger(ctx).Err(err).Msgf("failed to merge into table %s from %s", s.TableName, updatesFilename)

			return errors.Errorf("failed to merge into table %s from %s: %w", s.TableName, updatesFilename, err)
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

func (s *SnowflakeCSV) buildSetList(table1, table2 string, cols []string, mode setListMode) string {
	var ret []string
	createdAtCol := fmt.Sprintf("%s_created_at", s.Prefix)
	updatedAtCol := fmt.Sprintf("%s_updated_at", s.Prefix)
	for _, colName := range cols {
		// do not overwrite created_at on updates & deletes
		if colName == createdAtCol && (mode == updateSetMode || mode == deleteSetMode) {
			continue
		}
		// do not overwrite updated_at on deletes
		if colName == updatedAtCol && mode == deleteSetMode {
			continue
		}
		ret = append(ret, fmt.Sprintf("%s.%s = %s.%s", table1, colName, table2, colName))
	}

	return strings.Join(ret, ", ")
}

func buildSelectMerge(cols []string) string {
	ret := make([]string, len(cols))
	for i, colName := range cols {
		ret[i] = fmt.Sprintf("$%d %s", i+1, colName)
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
