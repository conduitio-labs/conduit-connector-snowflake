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

//go:generate mockgen -typed -destination=mock/writer.go -package=mock . Writer

package writer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/compress"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema/snowflake"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

var ErrTableNotFound = errors.New("table not found")

// SnowflakeCSV writer stores batch bytes into an SnowflakeCSV bucket as a file.
type SnowflakeCSV struct {
	prefix            string
	stage             string
	fileThreads       int
	processingWorkers int
	tableCache        map[string]snowflake.Table

	db         *sql.DB
	buffers    sync.Pool
	compressor compress.Compressor

	tableFunc func(sdk.Record) (string, error)
}

var _ Writer = (*SnowflakeCSV)(nil)

// SnowflakeConfig is a type used to initialize an Snowflake Writer.
type SnowflakeConfig struct {
	Prefix            string
	Stage             string
	TableName         string
	Connection        string
	ProcessingWorkers int
	FileThreads       int
	Compression       string
}

// tableFunction returns a function that determines the table for each record individually.
// The function might be returning a static table name.
// If the table is neither static nor a template, an error is returned.
func (c SnowflakeConfig) tableFunction() (func(sdk.Record) (string, error), error) {
	// Not a template, i.e. it's a static table name
	if !strings.HasPrefix(c.TableName, "{{") && !strings.HasSuffix(c.TableName, "}}") {
		return func(_ sdk.Record) (string, error) {
			return c.TableName, nil
		}, nil
	}

	// Try to parse the table
	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.TableName)
	if err != nil {
		// The table is not a valid Go template.
		return nil, fmt.Errorf("table is neither a valid static table nor a valid Go template: %w", err)
	}

	// The table is a valid template, return TableFn.
	var buf bytes.Buffer
	return func(r sdk.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute table template: %w", err)
		}
		return buf.String(), nil
	}, nil
}

// NewCSV takes an SnowflakeConfig reference and produces an SnowflakeCSV Writer.
func NewCSV(ctx context.Context, cfg SnowflakeConfig) (*SnowflakeCSV, error) {
	db, err := sql.Open("snowflake", cfg.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to snowflake db")
	}

	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf("CREATE OR REPLACE STAGE %s", cfg.Stage),
	); err != nil {
		return nil, fmt.Errorf("failed to create stage %q: %w", cfg.Stage, err)
	}

	if _, err := db.ExecContext(
		ctx,
		`
			CREATE OR REPLACE FILE FORMAT CSV_CONDUIT_SNOWFLAKE
				TYPE = 'csv'
				FIELD_DELIMITER = ','
				FIELD_OPTIONALLY_ENCLOSED_BY='"'
		`,
	); err != nil {
		return nil, fmt.Errorf("failed to create custom csv file format: %w", err)
	}

	if _, err := db.ExecContext(ctx, "ALTER SESSION SET TIMEZONE = 'UTC'"); err != nil {
		return nil, fmt.Errorf("failed to set session timezone to UTC: %w", err)
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
		return nil, fmt.Errorf("unrecognized compression type %q", cfg.Compression)
	}

	tableFn, err := cfg.tableFunction()
	if err != nil {
		return nil, fmt.Errorf("failed to determine table function: %w", err)
	}

	return &SnowflakeCSV{
		prefix:            cfg.Prefix,
		stage:             cfg.Stage,
		processingWorkers: cfg.ProcessingWorkers,
		fileThreads:       cfg.FileThreads,
		db:                db,
		compressor:        cmper,
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(nil)
			},
		},
		tableCache: make(map[string]snowflake.Table),
		tableFunc:  tableFn,
	}, nil
}

func (s *SnowflakeCSV) Close(ctx context.Context) error {
	dropStageQuery := fmt.Sprintf("DROP STAGE %s", s.stage)
	sdk.Logger(ctx).Debug().Msgf("executing: %s", dropStageQuery)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("DROP STAGE %s", s.stage)); err != nil {
		return fmt.Errorf("failed to drop stage: %w", err)
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

func (s *SnowflakeCSV) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// assign request id to the write cycle
	requestID := uuid.Must(uuid.NewV7()).String()

	ctx = sf.WithRequestID(ctx, sf.ParseUUID(requestID))
	logger := sdk.Logger(ctx).With().Str("request_id", requestID).Logger()

	// if s.schema == nil {
	// 	if err := s.initSchema(ctx, records); err != nil {
	// 		return 0, fmt.Errorf("failed to initialize schema from records: %w", err)
	// 	}

	// 	// N.B. Disable until table is created by the migrator
	// 	//
	// 	// migrated, err := s.evolver.Migrate(ctx, s.TableName, s.schema)
	// 	// if err != nil {
	// 	//	return 0, fmt.Errorf("failed to evolve schema during boot: %w", err)
	// 	// }

	// 	sdk.Logger(ctx).Debug().
	// 		// Bool("success", migrated).
	// 		Msg("schema initialized and migration completed")
	// }

	// log first record temporarily for debugging
	logger.Debug().Msgf("payload=%+v", records[0].Payload)
	logger.Debug().Msgf("payload.before=%+v", records[0].Payload.Before)
	logger.Debug().Msgf("payload.after=%+v", records[0].Payload.After)
	logger.Debug().Msgf("key=%+v", records[0].Key)

	batches, err := s.BuildBatches(ctx, records)
	if err != nil {
		return 0, fmt.Errorf("failed to build batches: %w", err)
	}

	buf := s.buffers.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		s.buffers.Put(buf)
	}()
	for _, batch := range batches {
		// TODO prepare the CSVs and upload it to Snowflake, after the CSVs for
		//  all batches are uploaded, start executing the merge queries in a
		//  transaction.

		// extract schema from payload
		recordTable, err := format.GetDataSchema(ctx, batch.Records[0], s.prefix)
		if err != nil {
			return 0, fmt.Errorf("failed to get data schema: %w", err)
		}

		// check if table already exists on snowflake, if yes, compare schema
		err = s.CheckTable(ctx, batch.Records[0].Operation, batch.Table, recordTable)
		if err != nil {
			if errors.Is(err, ErrTableNotFound) {
				// table doesn't exist, create it
				batch.Table, err = s.SetupTable(ctx, recordTable)
				if err != nil {
					return 0, fmt.Errorf("failed to set up snowflake table %q: %w", batch.Table.Name, err)
				}
			} else {
				return 0, fmt.Errorf("table %q already exists on snowflake but has an unexpected schema: %w", batch.Table.Name, err)
			}
		}

		err = format.MakeCSVBytes(
			ctx,
			records,
			batch.Table,
			buf,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to convert records to CSV: %w", err)
		}

		logger.Debug().
			Int("buf_len", buf.Len()).
			Msg("preparing to upload data to stage")

		if err := s.upload(ctx, batch.Filename, buf); err != nil {
			return 0, fmt.Errorf("failed to upload file %q to stage %q: %w", batch.Filename, s.stage, err)
		}
	}

	err = s.Merge(ctx, batches)
	if err != nil {
		return 0, fmt.Errorf("failed to merge batches: %w", err)
	}

	return len(records), nil
}

func (s *SnowflakeCSV) BuildBatches(ctx context.Context, records []sdk.Record) ([]*Batch, error) {
	batches := make(map[string]map[BatchType]*Batch)

	for _, r := range records {
		tableName, err := s.tableFunc(r)
		if err != nil {
			return nil, fmt.Errorf("failed to determine table name for record with position %s: %w", r.Position, err)
		}

		t, ok := s.tableCache[tableName]
		if !ok {
			t, err = s.FetchTable(ctx, tableName)
			if err != nil {
				if !errors.Is(err, ErrTableNotFound) {
					return nil, fmt.Errorf("failed to fetch table %q: %w", tableName, err)
				}
				// table doesn't exist, it will be created later
			}
			s.tableCache[tableName] = t
		}

		batchGroup, ok := batches[tableName]
		if !ok {
			batchGroup = make(map[BatchType]*Batch)
			batches[tableName] = batchGroup
		}

		var b *Batch
		switch r.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			b, ok = batchGroup[InsertBatch]
			if !ok {
				b = NewInsertBatch(uuid.NewString(), s.stage, t)
				batches[tableName][InsertBatch] = b
			}
		case sdk.OperationUpdate, sdk.OperationDelete:
			b, ok = batchGroup[UpdateBatch]
			if !ok {
				b = NewUpdateBatch(uuid.NewString(), s.stage, t)
				batches[tableName][UpdateBatch] = b
			}
		default:
			return nil, fmt.Errorf("unsupported operation %q", r.Operation)
		}

		b.Records = append(b.Records, r)
	}

	out := make([]*Batch, 0, len(batches)*2)
	for _, batchGroup := range maps.Values(batches) {
		out = append(out, maps.Values(batchGroup)...)
	}
	return out, nil
}

func (s *SnowflakeCSV) FetchTable(ctx context.Context, tableName string) (snowflake.Table, error) {
	logger := sdk.Logger(ctx).With().Str("table", tableName).Logger()
	var t snowflake.Table

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		txErr := tx.Rollback()
		if txErr != nil && !errors.Is(err, sql.ErrTxDone) {
			logger.Err(txErr).Msg("failed to roll back transaction")
		}
	}()

	// First check if table even exists and fetch its full qualified name.

	showTablesQuery := fmt.Sprintf(`SHOW TABLES LIKE '%s'`, tableName)
	logger.Debug().Str("query", "show tables").Msg(showTablesQuery)
	_, err = tx.ExecContext(ctx, showTablesQuery)
	if err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to check if table exists: %w", err)
	}

	selectTableRow := tx.QueryRowContext(ctx, `SELECT "name","database_name","schema_name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`)
	err = selectTableRow.Scan(&t.Name, &t.Database, &t.Schema)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snowflake.Table{}, ErrTableNotFound // Special case which we can handle.
		}
		return snowflake.Table{}, fmt.Errorf("failed to query table metadata: %w", err)
	}

	// Table exists, fetch all columns in the table.

	showColumnsQuery := fmt.Sprintf(`SHOW COLUMNS IN TABLE %s.%s.%s`, t.Database, t.Schema, t.Name)
	logger.Debug().Str("query", "show columns").Msg(showColumnsQuery)
	if _, err := s.db.Exec(showColumnsQuery); err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to query table columns: %w", err)
	}

	// This is ugly, but recommended by snowflake
	// https://community.snowflake.com/s/article/Select-the-list-of-columns-in-the-table-without-using-information-schema
	selectColumnsResult, err := tx.QueryContext(ctx, `SELECT "column_name","data_type" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY "column_name"`)
	if err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to select table columns: %w", err)
	}
	defer selectColumnsResult.Close()

	for selectColumnsResult.Next() {
		var colName, dataType string
		if err := selectColumnsResult.Scan(&colName, &dataType); err != nil {
			return snowflake.Table{}, fmt.Errorf("failed to scan column: %w", err)
		}

		var dt snowflake.DataTypeContainer
		if err := json.Unmarshal([]byte(dataType), &dt); err != nil {
			return snowflake.Table{}, fmt.Errorf("failed to unmarshal column type: %w", err)
		}

		col := snowflake.Column{
			Name:     colName,
			DataType: dt.Unmarshalled,
		}
		t.Columns = append(t.Columns, col)
		if strings.HasPrefix(colName, s.prefix) {
			switch colName {
			case s.prefix + "_operation":
				t.Operation = col
			case s.prefix + "_created_at":
				t.CreatedAt = col
			case s.prefix + "_updated_at":
				t.UpdatedAt = col
			case s.prefix + "_deleted_at":
				t.DeletedAt = col
			}
		}

		// // ensure that scale is non-zero to determine if it's an integer or not
		// if datatype == format.SnowflakeFixed {
		// 	scale := datatypeMap["scale"].(float64)
		// 	if scale > 0 {
		// 		finalType = format.SnowflakeFloat
		// 	} else {
		// 		finalType = format.SnowflakeInteger
		// 	}
		// } else {
		// 	finalType = format.SnowflakeTypeMapping[datatype]
		// }
		//
		// snowflakeSchema[strings.ToLower(columnName)] = finalType
	}

	if selectColumnsResult.Err() != nil {
		return snowflake.Table{}, fmt.Errorf("failed to iterate over columns: %w", selectColumnsResult.Err())
	}

	// At last, fetch primary keys of the table.

	showPrimaryKeysQuery := fmt.Sprintf(`SHOW PRIMARY KEYS IN TABLE %s.%s.%s`, t.Database, t.Schema, t.Name)
	logger.Debug().Str("query", "show primary keys").Msg(showPrimaryKeysQuery)
	if _, err := s.db.Exec(showPrimaryKeysQuery); err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to query primary keys: %w", err)
	}

	selectPrimaryKeysResult, err := tx.QueryContext(ctx, `SELECT "column_name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY "column_name"`)
	if err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to select primary keys: %w", err)
	}
	defer selectPrimaryKeysResult.Close()

	for selectPrimaryKeysResult.Next() {
		var colName string
		if err := selectPrimaryKeysResult.Scan(&colName); err != nil {
			return snowflake.Table{}, fmt.Errorf("failed to scan primary key: %w", err)
		}

		for i, col := range t.Columns {
			if col.Name == colName {
				t.PrimaryKeys = append(t.PrimaryKeys, t.Columns[i])
				continue
			}
			return snowflake.Table{}, fmt.Errorf("failed to find primary key column %q in table %q", colName, t.Name)
		}
	}
	if selectPrimaryKeysResult.Err() != nil {
		return snowflake.Table{}, fmt.Errorf("failed to iterate over primary keys: %w", selectPrimaryKeysResult.Err())
	}

	if err := tx.Commit(); err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return t, nil
}

func (s *SnowflakeCSV) CheckTable(
	ctx context.Context,
	operation sdk.Operation,
	snowflakeSchema snowflake.Table,
	recordSchema snowflake.Table,
) error {
	// if snowflake schema is empty, no need to check anything
	if len(snowflakeSchema.Columns) == 0 {
		return ErrTableNotFound
	}

	sdk.Logger(ctx).Debug().Msgf("Existing Table Schema (%+v)", snowflakeSchema.Columns)
	sdk.Logger(ctx).Debug().Msgf("Connector Generated Schema (%+v)", recordSchema.Columns)

	// Primary keys need to match either way.
	err := columnsMatch(recordSchema.PrimaryKeys, snowflakeSchema.PrimaryKeys)
	if err != nil {
		return fmt.Errorf("primary keys do not match: %w", err)
	}
	if operation != sdk.OperationDelete {
		// if operation is not delete, all columns should match
		err := columnsMatch(recordSchema.Columns, snowflakeSchema.Columns)
		if err != nil {
			return fmt.Errorf("columns do not match: %w", err)
		}
	}

	return nil
}

func columnsMatch(cols1, cols2 []snowflake.Column) error {
	if len(cols1) != len(cols2) {
		return fmt.Errorf("number of columns doesn't match (%d != %d)", len(cols1), len(cols2))
	}
	for k, col1 := range cols1 {
		col2 := cols2[k]
		if !strings.EqualFold(col1.Name, col2.Name) {
			return fmt.Errorf("column %d doesn't match (%s:%T != %s:%T)", k, col1.Name, col1.DataType, col2.Name, col2.DataType)
		}
		// TODO check data type? what if the source record has nil values and we don't know types?
	}

	return nil
}

// creates temporary, and destination table if they don't exist already.
func (s *SnowflakeCSV) SetupTable(
	ctx context.Context,
	table snowflake.Table,
) (snowflake.Table, error) {
	columnsSQL := buildSchema(table)

	queryCreateTable := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
    %s,
    PRIMARY KEY (%s)
)`, table.Name, columnsSQL, table.PrimaryKeys[0].Name) // TODO add support for composite keys

	sdk.Logger(ctx).Debug().Msgf("executing: %s", queryCreateTable)

	// We don't have to use a transaction here. Snowflake docs:
	// > Inside a transaction, any DDL statement (including CREATE TEMPORARY/TRANSIENT TABLE)
	//   commits the transaction before executing the DDL statement itself. The DDL statement
	//   then runs in its own transaction. The next statement after the DDL statement starts
	//   a new transaction.
	if _, err := s.db.ExecContext(ctx, queryCreateTable); err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to create destination table: %w", err)
	}

	return s.FetchTable(ctx, table.Name)
}

func (s *SnowflakeCSV) upload(ctx context.Context, filename string, buf *bytes.Buffer) error {
	start := time.Now()

	compressed := s.compressor.Compress(buf)

	ctx = sf.WithFileStream(ctx, compressed)
	ctx = sf.WithFileTransferOptions(ctx, &sf.SnowflakeFileTransferOptions{
		RaisePutGetError: true,
	})

	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"PUT file://%s @%s SOURCE_COMPRESSION=%s PARALLEL=%d",
		filename,
		s.stage,
		s.compressor.Name(),
		s.fileThreads,
	)); err != nil {
		return fmt.Errorf("failed to upload %q to stage %q: %w", filename, s.stage, err)
	}

	sdk.Logger(ctx).Debug().
		Dur("duration", time.Since(start)).
		Msgf("finished uploading file %s", filename)

	return nil
}

func (s *SnowflakeCSV) Merge(
	ctx context.Context,
	batches []*Batch,
) error {
	logger := sdk.Logger(ctx)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		txErr := tx.Rollback()
		if txErr != nil && !errors.Is(err, sql.ErrTxDone) {
			logger.Err(txErr).Msg("failed to roll back transaction")
		}
	}()

	logger.Debug().Msg("start of merge")
	for _, batch := range batches {
		batchLogger := logger.With().
			Str("table", batch.Table.Name).
			Str("file", batch.Filename).
			Logger()

		batchLogger.Debug().Msg("merging batch")

		mergeQuery, err := batch.MergeQuery()
		if err != nil {
			return fmt.Errorf("failed to construct merge query: %w", err)
		}

		logger.Debug().Str("query", "merge").Msg(mergeQuery)
		res, err := tx.ExecContext(ctx, mergeQuery)
		if err != nil {
			return fmt.Errorf("failed to merge into table %s from %s: %w", batch.Table.Name, batch.Filename, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			logger.Err(err).
				Msgf("could not determine rows affected on merge into table %s from %s", batch.Table.Name, batch.Filename)
		}

		batchLogger.Info().Int64("rows affected", rowsAffected).Msgf("MERGE successful")
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// initSchema creates a schema definition from the first record which has the value schema.
// func (s *SnowflakeCSV) initSchema(ctx context.Context, records []sdk.Record) error {
// 	start := time.Now()

// 	i := slices.IndexFunc(records, func(r sdk.Record) bool {
// 		return r.Metadata != nil && r.Metadata[valueSchema] != ""
// 	})

// 	if i < 0 {
// 		return fmt.Errorf("failed to find record with schema")
// 	}

// 	if ks, ok := records[i].Metadata[keySchema]; ok {
// 		_ = ks // do something with it
// 	}

// 	ksch, err := schema.ParseKafkaConnect(records[i].Metadata[valueSchema])
// 	if err != nil {
// 		return fmt.Errorf("failed to parse kafka schema: %w", err)
// 	}

// 	sch, err := schema.New(ksch)
// 	if err != nil {
// 		return fmt.Errorf("failed to construct avro schema: %w", err)
// 	}

// 	sdk.Logger(ctx).Debug().
// 		Str("schema", fmt.Sprint(sch)).
// 		Dur("duration", time.Since(start)).
// 		Msg("schema created")

// 	s.schema = sch

// 	return nil
// }

func buildSchema(table snowflake.Table) string {
	cols := make([]string, len(table.Columns))

	// we use the column order to make the query string deterministic
	for i, col := range table.Columns {
		cols[i] = col.Name + " " + col.DataType.SQLType()
	}

	return strings.Join(cols, ", ")
}
