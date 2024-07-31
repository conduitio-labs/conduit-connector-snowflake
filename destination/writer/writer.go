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
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/common"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/compress"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	csvFileFormatName = "CSV_CONDUIT_SNOWFLAKE"
)

// Writer is an interface that is responsible for persisting record that Destination
// has accumulated in its buffers. The default writer the Destination would use is
// SnowflakeWriter, others exists to test local behavior.
type Writer interface {
	Write(context.Context, []opencdc.Record) (int, error)
	Close(context.Context) error
}

// SnowflakeCSV writer stores batch bytes into an SnowflakeCSV bucket as a file.
type SnowflakeCSV struct {
	config SnowflakeConfig

	db *sql.DB

	insertsBuf    *bytes.Buffer
	updatesBuf    *bytes.Buffer
	compressedBuf *bytes.Buffer

	compressor compress.Compressor
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
	DSN               string
	Prefix            string
	PrimaryKey        string
	Stage             string
	Table             string
	ProcessingWorkers int
	FileThreads       int
	Compression       string
	CleanStageFiles   bool
}

// NewCSV takes an SnowflakeConfig reference and produces an SnowflakeCSV Writer.
func NewCSV(ctx context.Context, cfg SnowflakeConfig) (*SnowflakeCSV, error) {
	db, err := sql.Open("snowflake", cfg.DSN)
	if err != nil {
		return nil, errors.Errorf("failed to connect to snowflake db")
	}

	cmper, err := compress.New(cfg.Compression)
	if err != nil {
		return nil, err
	}

	s := &SnowflakeCSV{
		config:        cfg,
		db:            db,
		compressor:    cmper,
		insertsBuf:    &bytes.Buffer{},
		updatesBuf:    &bytes.Buffer{},
		compressedBuf: &bytes.Buffer{},
	}

	if err := s.initStage(ctx); err != nil {
		return nil, err
	}

	if err := s.initFormats(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SnowflakeCSV) Close(ctx context.Context) error {
	if _, err := s.db.ExecContext(
		ctx,
		"DROP STAGE "+s.config.Stage,
	); err != nil {
		return errors.Errorf("failed to drop stage %q: %w", s.config.Stage, err)
	}

	if err := s.db.Close(); err != nil {
		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

func (s *SnowflakeCSV) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	var err error
	ctx = common.WithRequestID(ctx)

	// extract schema from payload
	schema := make(map[string]string)
	csvColumnOrder, meroxaColumns, err := format.GetDataSchema(ctx, records, schema, s.config.Prefix)
	if err != nil {
		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// check if table already exists on snowflake, if yes, compare schema
	err = s.CheckTable(ctx, records[0].Operation, s.config.PrimaryKey, schema)
	if err != nil {
		return 0, errors.Errorf("failed to checking table %q on snowflake: %w", s.config.Table, err)
	}

	err = format.MakeCSVBytes(
		ctx,
		records,
		csvColumnOrder,
		*meroxaColumns,
		schema,
		s.config.PrimaryKey,
		s.insertsBuf,
		s.updatesBuf,
		s.config.ProcessingWorkers,
	)
	if err != nil {
		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// Clean out the buffers after write. Storage will be reused next.
	defer func() {
		s.insertsBuf.Reset()
		s.updatesBuf.Reset()
	}()

	err = s.SetupTables(ctx, schema, csvColumnOrder)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake table %q: %w", s.config.Table, err)
	}

	sdk.Logger(ctx).Debug().
		Int("insertbuf_len", s.insertsBuf.Len()).
		Int("updatebuf_len", s.updatesBuf.Len()).
		Msg("preparing to upload data to stage")

	var insertsFilename, updatesFilename string

	if s.insertsBuf.Len() > 0 {
		insertsFilename = fmt.Sprintf("%s_inserts.csv.%s", common.CtxRequestID(ctx), s.compressor.Name())
		if err = s.upload(ctx, insertsFilename, s.insertsBuf); err != nil {
			return 0, errors.Errorf("failed to upload file %q to stage %q: %w", insertsFilename, s.config.Stage, err)
		}
	}

	if s.updatesBuf.Len() > 0 {
		updatesFilename = fmt.Sprintf("%s_updates.csv.%s", common.CtxRequestID(ctx), s.compressor.Name())
		if err := s.upload(ctx, updatesFilename, s.updatesBuf); err != nil {
			return 0, errors.Errorf("failed to upload file %q to stage %q: %w", updatesFilename, s.config.Stage, err)
		}
	}

	if err := s.Merge(ctx, insertsFilename, updatesFilename, csvColumnOrder, meroxaColumns); err != nil {
		return 0, errors.Errorf(
			"failed to merge uploaded stage files %q, %q: %w",
			insertsFilename,
			updatesFilename,
			err,
		)
	}

	if err := s.cleanupFiles(ctx, insertsFilename, updatesFilename); err != nil {
		sdk.Logger(ctx).Error().Err(err).
			Str("inserts_file", insertsFilename).
			Str("updates_file", updatesFilename).
			Msgf("failed to delete files from stage %q", s.config.Stage)
	}

	return len(records), nil
}

func (s *SnowflakeCSV) CheckTable(ctx context.Context, operation opencdc.Operation,
	primaryKey string, schema map[string]string,
) error {
	showTablesQuery := fmt.Sprintf(`SHOW TABLES LIKE '%s';`, s.config.Table)
	res, err := s.db.Query(showTablesQuery)
	if err != nil {
		return errors.Errorf("failed to check if table exists: %w", err)
	}

	defer res.Close()

	// table not found
	if !res.Next() {
		sdk.Logger(ctx).Info().Msgf("table %s does not exist yet", s.config.Table)

		return nil
	}

	if err = res.Err(); err != nil {
		return errors.Errorf("error occurred while checking table rows: %w", err)
	}

	showColumnsQuery := fmt.Sprintf(`SHOW COLUMNS IN TABLE %s;`, s.config.Table)

	sdk.Logger(ctx).Debug().Msgf("executing: %s", showColumnsQuery)

	if _, err := s.db.Exec(showColumnsQuery); err != nil {
		return errors.Errorf("failed to check if table exists: %w", err)
	}

	// TODO: wrap in a transaction, this is ugly, but unfortunately recommended by snowflake
	// https://community.snowflake.com/s/article/Select-the-list-of-columns-in-the-table-without-using-information-schema
	response, err := s.db.Query(`SELECT "column_name","data_type" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`)
	if err != nil {
		return errors.Errorf("failed to check if table exists: %w", err)
	}

	defer response.Close()

	snowflakeSchema := make(map[string]string)
	// grab columns from snowflake if they exist
	for response.Next() {
		var columnName, d, finalType string
		if err := response.Scan(&columnName, &d); err != nil {
			return err
		}

		datatypeMap := make(map[string]interface{})
		if err := json.Unmarshal([]byte(d), &datatypeMap); err != nil {
			return err
		}

		datatype := datatypeMap["type"].(string)

		// ensure that scale is non-zero to determine if it's an integer or not
		if datatype == format.SnowflakeFixed {
			scale := datatypeMap["scale"].(float64)
			if scale > 0 {
				finalType = format.SnowflakeFloat
			} else {
				finalType = format.SnowflakeInteger
			}
		} else {
			finalType = format.SnowflakeTypeMapping[datatype]
		}

		snowflakeSchema[strings.ToLower(columnName)] = finalType
	}

	if err := response.Err(); err != nil {
		return errors.Errorf("error grabbing columns: %w", err)
	}

	// if snowflake schema is empty, no need to check anything
	if len(snowflakeSchema) == 0 {
		return nil
	}

	sdk.Logger(ctx).Debug().Msgf("Existing Table Schema (%+v)", snowflakeSchema)
	sdk.Logger(ctx).Debug().Msgf("Connector Generated Schema (%+v)", schema)

	// if operation is delete, we want to ensure that primary key is on dest table as well as meroxa columns
	if operation == opencdc.OperationDelete {
		_, ok := snowflakeSchema[primaryKey]
		if ok {
			return nil
		}
		err := schemaMatches(schema, snowflakeSchema)
		if err != nil {
			return err
		}
	} else {
		// if its not delete, schemas should match exactly
		if len(schema) != len(snowflakeSchema) {
			sdk.Logger(ctx).Debug().Msgf("Snowflake Table Schema (%+v)", snowflakeSchema)
			sdk.Logger(ctx).Debug().Msgf("Source Table Schema (%+v)", schema)

			return errors.Errorf("table already exists on snowflake, source schema number of "+
				"columns %d doesn't match destination table %d",
				len(schema),
				len(snowflakeSchema))
		}

		err := schemaMatches(schema, snowflakeSchema)
		if err != nil {
			return err
		}
	}

	return nil
}

func schemaMatches(schema, snowflakeSchema map[string]string) error {
	for k, v := range schema {
		lowerK := strings.ToLower(k)
		v2, ok := snowflakeSchema[lowerK]
		if !ok {
			return errors.Errorf("table already exists on snowflake, column %s doesn't exist on destination table ", k)
		}
		if !strings.EqualFold(v, v2) {
			return errors.Errorf("table already exists on snowflake, column %s with datatype %s "+
				"doesn't match on destination table of datatype %s ", k, v, v2)
		}
	}

	return nil
}

// creates temporary, and destination table if they don't exist already.
func (s *SnowflakeCSV) SetupTables(ctx context.Context, schema map[string]string, columnOrder []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Errorf("failed to create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	columnsSQL := buildSchema(schema, columnOrder)

	queryCreateTable := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
		%s,
		PRIMARY KEY (%s)
	)`, s.config.Table, columnsSQL, s.config.PrimaryKey)

	sdk.Logger(ctx).Debug().Msgf("executing: %s", queryCreateTable)

	if _, err = tx.ExecContext(ctx, queryCreateTable); err != nil {
		return errors.Errorf("failed to create destination table: %w", err)
	}

	return tx.Commit()
}

func (s *SnowflakeCSV) upload(ctx context.Context, filename string, buf *bytes.Buffer) error {
	start := time.Now()
	sizein := buf.Len()

	if err := s.compressor.Compress(buf, s.compressedBuf); err != nil {
		return errors.Errorf("failed to compress buffer: %w", err)
	}
	defer s.compressedBuf.Reset()

	sdk.Logger(ctx).Debug().
		Dur("duration", time.Since(start)).
		Int("in", sizein).
		Int("out", s.compressedBuf.Len()).
		Msg("finished compressing")

	ctx = sf.WithFileStream(ctx, s.compressedBuf)
	ctx = sf.WithFileTransferOptions(ctx, &sf.SnowflakeFileTransferOptions{
		RaisePutGetError: true,
	})

	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"PUT file://%s @%s SOURCE_COMPRESSION=%s PARALLEL=%d",
		filename,
		s.config.Stage,
		s.compressor.Name(),
		s.config.FileThreads,
	)); err != nil {
		return errors.Errorf("failed to upload %q to stage %q: %w", filename, s.config.Stage, err)
	}

	sdk.Logger(ctx).Debug().
		Dur("duration", time.Since(start)).
		Msgf("finished uploading file %s", filename)

	return nil
}

// cleanupFiles will attempt remove the specified files from the stage, noop when config is disabled.
func (s *SnowflakeCSV) cleanupFiles(ctx context.Context, files ...string) error {
	if !s.config.CleanStageFiles {
		return nil
	}

	var errs error

	for _, file := range files {
		if file == "" {
			continue
		}

		sdk.Logger(ctx).Debug().Str("file", file).
			Msgf("cleaning file from stage %q", s.config.Stage)

		if _, err := s.db.ExecContext(
			ctx,
			fmt.Sprintf("REMOVE @%s", path.Join(s.config.Stage, file)),
		); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (s *SnowflakeCSV) Merge(
	ctx context.Context,
	insertsFilename,
	updatesFilename string,
	colOrder []string,
	meroxaColumns *format.ConnectorColumns,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Debug().Msg("start of merge")

	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			sdk.Logger(ctx).Err(err).Msg("rolling back transaction")
		}
	}()

	orderingColumnList := fmt.Sprintf("a.%s = b.%s", s.config.PrimaryKey, s.config.PrimaryKey)
	insertSetCols := s.buildSetList("a", "b", colOrder, insertSetMode, meroxaColumns)
	updateSetCols := s.buildSetList("a", "b", colOrder, updateSetMode, meroxaColumns)
	deleteSetCols := s.buildSetList("a", "b", colOrder, deleteSetMode, meroxaColumns)

	colListA := buildFinalColumnList("a", ".", colOrder)
	colListB := buildFinalColumnList("b", ".", colOrder)
	setSelectMerge := buildSelectMerge(colOrder)

	sdk.Logger(ctx).Debug().Msgf("insertsFilename=%s, updatesFilename=%s", insertsFilename, updatesFilename)

	if insertsFilename != "" {
		// MERGE for inserts
		sdk.Logger(ctx).Debug().Msg("constructing merge query for inserts")

		//nolint:gosec // not an issue
		queryMergeInto := fmt.Sprintf(
			`MERGE INTO %s as a USING ( select %s from @%s/%s (FILE_FORMAT =>  %s ) ) AS b ON %s
			WHEN MATCHED AND ( b.%s_operation = 'create' OR b.%s_operation = 'snapshot' ) THEN UPDATE SET %s
			WHEN NOT MATCHED AND ( b.%s_operation = 'create' OR b.%s_operation = 'snapshot' ) THEN INSERT  (%s) VALUES (%s) ; `,
			s.config.Table,
			setSelectMerge,
			s.config.Stage,
			insertsFilename,
			csvFileFormatName,
			orderingColumnList,
			// second line
			s.config.Prefix,
			s.config.Prefix,
			insertSetCols,
			// third line
			s.config.Prefix,
			s.config.Prefix,
			colListA,
			colListB,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", queryMergeInto)

		res, err := tx.ExecContext(ctx, queryMergeInto)
		if err != nil {
			return errors.Errorf("failed to merge into table %s from %s: %w", s.config.Table, insertsFilename, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			sdk.Logger(ctx).
				Err(err).
				Msgf("could not determine rows affected on merge into table %s from %s", s.config.Table, insertsFilename)
		}

		sdk.Logger(ctx).Info().Msgf("ran MERGE for inserts. rows affected: %d", rowsAffected)
	}

	if updatesFilename != "" {
		sdk.Logger(ctx).Debug().Msg("constructing merge query for update / delete")

		//nolint:gosec // not an issue
		queryMergeInto := fmt.Sprintf(
			`MERGE INTO %s as a USING ( select %s from @%s/%s (FILE_FORMAT =>  %s ) ) AS b ON %s
			WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s
			WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET %s
			WHEN NOT MATCHED AND b.%s_operation = 'update' THEN INSERT  (%s) VALUES (%s)
			WHEN NOT MATCHED AND b.%s_operation = 'delete' THEN INSERT  (%s) VALUES (%s) ; `,
			s.config.Table,
			setSelectMerge,
			s.config.Stage,
			updatesFilename,
			csvFileFormatName,
			orderingColumnList,
			// second line
			s.config.Prefix,
			updateSetCols,
			// third line
			s.config.Prefix,
			deleteSetCols,
			// fourth line
			s.config.Prefix,
			colListA,
			colListB,
			// fifth line
			s.config.Prefix,
			colListA,
			colListB,
		)

		sdk.Logger(ctx).Debug().Msgf("executing: %s", queryMergeInto)

		res, err := tx.ExecContext(ctx, queryMergeInto)
		if err != nil {
			return errors.Errorf("failed to merge into table %s from %s: %w", s.config.Table, updatesFilename, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			sdk.Logger(ctx).
				Err(err).
				Msgf("could not determine rows affected on merge into table %s from %s", s.config.Table, updatesFilename)
		}

		sdk.Logger(ctx).Info().Msgf("ran MERGE for updates/deletes. rows affected: %d", rowsAffected)
	}

	if err := tx.Commit(); err != nil {
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

func (s *SnowflakeCSV) buildSetList(t1, t2 string, cols []string, m setListMode, mCol *format.ConnectorColumns) string {
	var ret []string
	for _, colName := range cols {
		// for deletes, ONLY update deleted_at and operation
		if m == deleteSetMode && (colName != mCol.DeletedAtColumn && colName != mCol.OperationColumn) {
			continue
		}

		// do not overwrite created_at on updates
		if m == updateSetMode && colName == mCol.CreatedAtColumn {
			continue
		}

		ret = append(ret, fmt.Sprintf("%s.%s = %s.%s", t1, colName, t2, colName))
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

func buildSchema(schema map[string]string, columnOrder []string) string {
	cols := make([]string, len(schema))

	// we use the column order to make the query string determinstic
	for i, colName := range columnOrder {
		sqlType := schema[colName]
		cols[i] = strings.Join([]string{colName, sqlType}, " ")
	}

	return strings.Join(cols, ", ")
}

func (s *SnowflakeCSV) initStage(ctx context.Context) error {
	if _, err := s.db.ExecContext(
		ctx,
		fmt.Sprintf("CREATE OR REPLACE STAGE %s", s.config.Stage),
	); err != nil {
		return errors.Errorf("failed to create stage %q: %w", s.config.Stage, err)
	}

	return nil
}

func (s *SnowflakeCSV) initFormats(ctx context.Context) error {
	if _, err := s.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`CREATE OR REPLACE FILE FORMAT %s TYPE = 'csv' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY='"'`,
			csvFileFormatName,
		),
	); err != nil {
		return errors.Errorf("failed to create custom csv file format: %w", err)
	}

	return nil
}
