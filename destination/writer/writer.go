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
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/compress"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema"
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
	Write(context.Context, []sdk.Record) (int, error)
	Close(context.Context) error
}

// SnowflakeCSV writer stores batch bytes into an SnowflakeCSV bucket as a file.
type SnowflakeCSV struct {
	Prefix            string
	PrimaryKey        string
	Stage             string
	TableName         string
	FileThreads       int
	ProcessingWorkers int

	db *sql.DB

	evolver *schema.Evolver

	insertsBuf    *bytes.Buffer
	updatesBuf    *bytes.Buffer
	compressedBuf *bytes.Buffer

	compressor compress.Compressor
	// schema     schema.Schema
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
	Prefix            string
	PrimaryKey        string
	Stage             string
	TableName         string
	Connection        string
	ProcessingWorkers int
	FileThreads       int
	Compression       string
}

// NewCSV takes an SnowflakeConfig reference and produces an SnowflakeCSV Writer.
func NewCSV(ctx context.Context, cfg *SnowflakeConfig) (*SnowflakeCSV, error) {
	db, err := sql.Open("snowflake", cfg.Connection)
	if err != nil {
		return nil, errors.Errorf("failed to connect to snowflake db")
	}

	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf("CREATE OR REPLACE STAGE %s", cfg.Stage),
	); err != nil {
		return nil, errors.Errorf("failed to create stage %q: %w", cfg.Stage, err)
	}

	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`
			CREATE OR REPLACE FILE FORMAT %s
				TYPE = 'csv'
				FIELD_DELIMITER = ','
				FIELD_OPTIONALLY_ENCLOSED_BY='"'
		`, csvFileFormatName),
	); err != nil {
		return nil, errors.Errorf("failed to create custom csv file format: %w", err)
	}

	var cmper compress.Compressor

	switch cfg.Compression {
	case compress.TypeGzip:
		cmper = compress.Gzip{}
	case compress.TypeZstd:
		cmper = compress.Zstd{}
	case compress.TypeCopy:
		cmper = compress.Copy{}
	default:
		return nil, errors.Errorf("unrecognized compression type %q", cfg.Compression)
	}

	return &SnowflakeCSV{
		Prefix:            cfg.Prefix,
		PrimaryKey:        cfg.PrimaryKey,
		Stage:             cfg.Stage,
		TableName:         cfg.TableName,
		ProcessingWorkers: cfg.ProcessingWorkers,
		FileThreads:       cfg.FileThreads,
		db:                db,
		evolver:           schema.NewEvolver(db),
		compressor:        cmper,
		insertsBuf:        &bytes.Buffer{},
		updatesBuf:        &bytes.Buffer{},
		compressedBuf:     &bytes.Buffer{},
	}, nil
}

func (s *SnowflakeCSV) Close(ctx context.Context) error {
	dropStageQuery := fmt.Sprintf("DROP STAGE %s", s.Stage)
	sdk.Logger(ctx).Debug().Msgf("executing: %s", dropStageQuery)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("DROP STAGE %s", s.Stage)); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to drop stage")

		return errors.Errorf("failed to drop stage: %w", err)
	}
	if err := s.db.Close(); err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to gracefully close the connection")

		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

func (s *SnowflakeCSV) Write(ctx context.Context, records []sdk.Record) (int, error) {
	var err error
	// assign request id to the write cycle
	ctx = withRequestID(ctx)

	// if s.schema == nil {
	// 	if err := s.initSchema(ctx, records); err != nil {
	// 		return 0, errors.Errorf("failed to initialize schema from records: %w", err)
	// 	}

	// 	// N.B. Disable until table is created by the migrator
	// 	//
	// 	// migrated, err := s.evolver.Migrate(ctx, s.TableName, s.schema)
	// 	// if err != nil {
	// 	//	return 0, errors.Errorf("failed to evolve schema during boot: %w", err)
	// 	// }

	// 	sdk.Logger(ctx).Debug().
	// 		// Bool("success", migrated).
	// 		Msg("schema initialized and migration completed")
	// }

	// log first record temporarily for debugging
	sdk.Logger(ctx).Debug().Msgf("payload=%+v", records[0].Payload)
	sdk.Logger(ctx).Debug().Msgf("payload.before=%+v", records[0].Payload.Before)
	sdk.Logger(ctx).Debug().Msgf("payload.after=%+v", records[0].Payload.After)
	sdk.Logger(ctx).Debug().Msgf("key=%+v", records[0].Key)
	// extract schema from payload
	schema := make(map[string]string)
	csvColumnOrder, meroxaColumns, err := format.GetDataSchema(ctx, records, schema, s.Prefix)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to convert records to CSV")

		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// check if table already exists on snowflake, if yes, compare schema
	err = s.CheckTable(ctx, records[0].Operation, s.PrimaryKey, schema)
	if err != nil {
		return 0, errors.Errorf("failed to checking table %q on snowflake: %w", s.TableName, err)
	}

	err = format.MakeCSVBytes(
		ctx,
		records,
		csvColumnOrder,
		*meroxaColumns,
		s.PrimaryKey,
		s.insertsBuf,
		s.updatesBuf,
		s.ProcessingWorkers,
	)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to convert records to CSV")

		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// Clean out the buffers after write. Storage will be reused next.
	defer func() {
		s.insertsBuf.Reset()
		s.updatesBuf.Reset()
	}()

	err = s.SetupTables(ctx, schema, csvColumnOrder)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake table %q: %w", s.TableName, err)
	}

	sdk.Logger(ctx).Debug().
		Int("insertbuf_len", s.insertsBuf.Len()).
		Int("updatebuf_len", s.updatesBuf.Len()).
		Msg("preparing to upload data to stage")

	var insertsFilename, updatesFilename string

	if s.insertsBuf.Len() > 0 {
		insertsFilename = fmt.Sprintf("%s_inserts.csv.gz", requestID(ctx))
		if err = s.upload(ctx, insertsFilename, s.insertsBuf); err != nil {
			return 0, errors.Errorf("failed to upload file %q to stage %q: %w", insertsFilename, s.Stage, err)
		}
	}

	if s.updatesBuf.Len() > 0 {
		updatesFilename = fmt.Sprintf("%s_updates.csv.gz", requestID(ctx))
		if err := s.upload(ctx, updatesFilename, s.updatesBuf); err != nil {
			return 0, errors.Errorf("failed to upload file %q to stage %q: %w", updatesFilename, s.Stage, err)
		}
	}

	if err := s.Merge(ctx, insertsFilename, updatesFilename, csvColumnOrder); err != nil {
		return 0, errors.Errorf(
			"failed to merge uploaded stage files %q, %q: %w",
			insertsFilename,
			updatesFilename,
			err,
		)
	}

	return len(records), nil
}

func (s *SnowflakeCSV) CheckTable(ctx context.Context, operation sdk.Operation,
	primaryKey string, schema map[string]string,
) error {
	snowflakeSchema := make(map[string]string)
	var columnName, dataType, isNullable string

	//nolint:gosec // not an issue
	query := fmt.Sprintf(`
					SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE
					FROM INFORMATION_SCHEMA.COLUMNS c
					WHERE c.TABLE_NAME ilike '%s'
					ORDER BY c.ORDINAL_POSITION;
					`, s.TableName)

	sdk.Logger(ctx).Debug().Msgf("executing: %s", query)

	response, err := s.db.Query(query)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to check if table exists")

		return errors.Errorf("failed to check if table exists: %w", err)
	}

	defer response.Close()

	// grab columns from snowflake if they exist
	for response.Next() {
		err := response.Scan(&columnName, &dataType, &isNullable)
		if err != nil {
			return err
		}
		// snowflake stores varchar as text on schema info, we use varchar in our schema def
		if dataType == "TEXT" {
			dataType = "VARCHAR"
		}
		snowflakeSchema[strings.ToLower(columnName)] = dataType
	}

	if err := response.Err(); err != nil {
		sdk.Logger(ctx).Err(err).Msg("error grabbing columns")

		return errors.Errorf("error grabbing columns: %w", err)
	}

	// if snowflake schema is empty, no need to check anything
	if len(snowflakeSchema) == 0 {
		return nil
	}

	// if operation is delete, we want to ensure that primary key is on dest table as well as meroxa columns
	if operation == sdk.OperationDelete {
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
		if strings.EqualFold(v, v2) {
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
		sdk.Logger(ctx).Err(err).Msg("failed to begin transaction")

		return errors.Errorf("failed to create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	columnsSQL := buildSchema(schema, columnOrder)

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
		s.Stage,
		s.compressor.Name(),
		s.FileThreads,
	)); err != nil {
		return errors.Errorf("failed to upload %q to stage %q: %w", filename, s.Stage, err)
	}

	sdk.Logger(ctx).Debug().
		Dur("duration", time.Since(start)).
		Msgf("finished uploading file %s", filename)

	return nil
}

func (s *SnowflakeCSV) Merge(
	ctx context.Context,
	insertsFilename,
	updatesFilename string,
	colOrder []string,
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

	orderingColumnList := fmt.Sprintf("a.%s = b.%s", s.PrimaryKey, s.PrimaryKey)
	insertSetCols := s.buildSetList("a", "b", colOrder, insertSetMode)
	updateSetCols := s.buildSetList("a", "b", colOrder, updateSetMode)
	deleteSetCols := s.buildSetList("a", "b", colOrder, deleteSetMode)

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
			s.TableName,
			setSelectMerge,
			s.Stage,
			insertsFilename,
			csvFileFormatName,
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
			`MERGE INTO %s as a USING ( select %s from @%s/%s (FILE_FORMAT =>  %s ) ) AS b ON %s
			WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s
			WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET %s
			WHEN NOT MATCHED AND b.%s_operation = 'update' THEN INSERT  (%s) VALUES (%s)
			WHEN NOT MATCHED AND b.%s_operation = 'delete' THEN INSERT  (%s) VALUES (%s) ; `,
			s.TableName,
			setSelectMerge,
			s.Stage,
			updatesFilename,
			csvFileFormatName,
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

// initSchema creates a schema definition from the first record which has the value schema.
// func (s *SnowflakeCSV) initSchema(ctx context.Context, records []sdk.Record) error {
// 	start := time.Now()

// 	i := slices.IndexFunc(records, func(r sdk.Record) bool {
// 		return r.Metadata != nil && r.Metadata[valueSchema] != ""
// 	})

// 	if i < 0 {
// 		return errors.Errorf("failed to find record with schema")
// 	}

// 	if ks, ok := records[i].Metadata[keySchema]; ok {
// 		_ = ks // do something with it
// 	}

// 	ksch, err := schema.ParseKafkaConnect(records[i].Metadata[valueSchema])
// 	if err != nil {
// 		return errors.Errorf("failed to parse kafka schema: %w", err)
// 	}

// 	sch, err := schema.New(ksch)
// 	if err != nil {
// 		return errors.Errorf("failed to construct avro schema: %w", err)
// 	}

// 	sdk.Logger(ctx).Debug().
// 		Str("schema", fmt.Sprint(sch)).
// 		Dur("duration", time.Since(start)).
// 		Msg("schema created")

// 	s.schema = sch

// 	return nil
// }

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
