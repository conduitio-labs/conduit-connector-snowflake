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
	"github.com/huandu/go-sqlbuilder"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

// Writer is an interface that is responsible for persisting record that Destination
// has accumulated in its buffers. The default writer the Destination would use is
// SnowflakeWriter, others exists to test local behavior.
type Writer interface {
	Write(context.Context, []sdk.Record) (int, error)
	Close() error
}

// Snowflake writer stores batch bytes into an Snowflake bucket as a file.
type Snowflake struct {
	Prefix      string
	PrimaryKey  []string
	Stage       string
	TableName   string
	Format      format.Format
	SnowflakeDB *sql.DB
}

var _ Writer = (*Snowflake)(nil)

// SnowflakeConfig is a type used to initialize an Snowflake Writer.
type SnowflakeConfig struct {
	Prefix     string
	PrimaryKey []string
	Stage      string
	TableName  string
	Connection string
	Format     format.Format
}

// NewSnowflake takes an SnowflakeConfig reference and produces an Snowflake Writer.
func NewSnowflake(ctx context.Context, cfg *SnowflakeConfig) (*Snowflake, error) {
	db, err := sql.Open("snowflake", cfg.Connection)
	if err != nil {
		return nil, errors.Errorf("failed to connect to snowflake db")
	}

	// create the stage if it doesn't exist
	if _, err := db.Exec(fmt.Sprintf("CREATE STAGE IF NOT EXISTS %s", cfg.Stage)); err != nil {
		return nil, err
	}

	return &Snowflake{
		Prefix:      cfg.Prefix,
		PrimaryKey:  cfg.PrimaryKey,
		SnowflakeDB: db,
		Stage:       cfg.Stage,
		TableName:   cfg.TableName,
		Format:      cfg.Format,
	}, nil
}

func (s *Snowflake) Close() error {
	if err := s.SnowflakeDB.Close(); err != nil {
		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

// Write stores the batch on AWS Snowflake as a file.
func (s *Snowflake) Write(ctx context.Context, records []sdk.Record) (int, error) {
	var (
		schema     map[string]string
		indexCols  []string
		colOrder   []string
		insertsBuf *bytes.Buffer
		updatesBuf *bytes.Buffer
		err        error
	)

	insertsBuf, updatesBuf, schema, indexCols, colOrder, err = s.Format.MakeBytes(records, s.Prefix, s.PrimaryKey)
	if err != nil {
		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// generate a UUID used for the temporary table and filename in internal stage
	batchUUID := strings.Replace(uuid.NewString(), "-", "", -1)
	var insertsFilename, updatesFilename string

	tempTable, err := s.SetupTables(ctx, batchUUID, schema)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake tables: %w", err)
	}

	sdk.Logger(ctx).Debug().Msg("set up necessary tables")
	sdk.Logger(ctx).Debug().Msgf("insertBuffer.Len()=%d, updatesBuf.Len()=%d", insertsBuf.Len(), updatesBuf.Len())

	if insertsBuf != nil && insertsBuf.Len() > 0 {
		insertsFilename = fmt.Sprintf("inserts_%s.csv", batchUUID)
		if err := s.PutFileInStage(ctx, insertsBuf, insertsFilename); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
	}

	if updatesBuf != nil && updatesBuf.Len() > 0 {
		updatesFilename = fmt.Sprintf("updates_%s.csv", batchUUID)
		if err := s.PutFileInStage(ctx, updatesBuf, updatesFilename); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
	}

	if err := s.CopyAndMerge(ctx, tempTable, insertsFilename, updatesFilename, colOrder, indexCols, schema); err != nil {
		return 0, errors.Errorf("failed to process records:")
	}

	return len(records), nil
}

// creates temporary, and destination table if they don't exist already.
func (s *Snowflake) SetupTables(ctx context.Context, batchUUID string, schema map[string]string) (string, error) {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	tempTable := fmt.Sprintf("%s_temp_%s", s.TableName, batchUUID)
	columnsSQL := buildSchema(schema)
	queryCreateTempTable := fmt.Sprintf(
		`CREATE TEMPORARY TABLE IF NOT EXISTS %s (%s)`,
		tempTable, columnsSQL)
	if _, err = tx.ExecContext(ctx, buildQuery(ctx, queryCreateTempTable)); err != nil {
		return "", errors.Errorf("create temporary table: %w", err)
	}

	queryCreateTable := `CREATE TABLE IF NOT EXISTS %s (
		%s,
		meroxa_deleted_at TIMESTAMP_LTZ,
		meroxa_created_at TIMESTAMP_LTZ,
		meroxa_updated_at TIMESTAMP_LTZ )`

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTable, s.TableName, columnsSQL))); err != nil {
		return "", errors.Errorf("create destination table: %w", err)
	}

	return tempTable, tx.Commit()
}

func (s *Snowflake) PutFileInStage(ctx context.Context, buf *bytes.Buffer, filename string) error {
	// nolint:errcheck,nolintlint
	q := fmt.Sprintf(
		"PUT file://%s @%s auto_compress=true parallel=30;", // TODO: make parallelism configurable.
		filename,
		s.Stage,
	)

	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	ctxFs := sf.WithFileStream(ctx, buf)
	ctxFs = sf.WithFileTransferOptions(ctxFs, &sf.SnowflakeFileTransferOptions{
		RaisePutGetError: true,
	})

	if _, err := tx.ExecContext(ctxFs, q); err != nil {
		return errors.Errorf("PUT file %s in stage %s: %w", filename, s.Stage, err)
	}

	if err := tx.Commit(); err != nil {
		return errors.Errorf("error putting file in stage: %w", err)
	}

	return nil
}

func (s *Snowflake) CopyAndMerge(ctx context.Context, tempTable, insertsFilename, updatesFilename string,
	colOrder, indexCols []string, schema map[string]string) error {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
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
		aliasCols := toStr(aliasFields)
		query := fmt.Sprintf(`
			COPY INTO %s (%s, %s_created_at)
			FROM (
				SELECT %s, CURRENT_TIMESTAMP()
				FROM @%s/%s f
			)
			FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);`,
			s.TableName, colList, s.Prefix, aliasCols, s.Stage, insertsFilename,
		)

		sdk.Logger(ctx).Debug().Msgf("query constructed for COPY INTO for inserts: %s", query)

		if _, err = tx.ExecContext(ctx, query); err != nil {
			return errors.Errorf("failed to copy file %s in %s: %w", insertsFilename, s.TableName, err)
		}
		sdk.Logger(ctx).Info().Msg("ran COPY INTO for inserts")
	}

	if updatesFilename != "" {
		// COPY INTO for updates
		queryCopyInto := fmt.Sprintf(`
			COPY INTO %s FROM @%s
			FILES = ('%s.gz')
			FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  PARSE_HEADER = TRUE)
			MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE' PURGE = TRUE;`,
			tempTable,
			s.Stage,
			updatesFilename,
		)

		if _, err = tx.ExecContext(ctx, queryCopyInto); err != nil {
			return errors.Errorf("failed to copy file %s in temp %s: %w", updatesFilename, tempTable, err)
		}

		sdk.Logger(ctx).Info().Msg("ran COPY INTO for updates/deletes")

		// MERGE
		cols := maps.Keys(schema)
		updateSet := buildOrderingColumnList("a", "b", ",", cols)
		orderingColumnList := buildOrderingColumnList("a", "b", " AND ", indexCols)

		queryMergeInto := `MERGE INTO %s as a USING %s AS b ON %s
			WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s, a.%s_updated_at = CURRENT_TIMESTAMP()
			WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET a.%s_deleted_at = CURRENT_TIMESTAMP()`

		if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryMergeInto,
			s.TableName,
			tempTable,
			orderingColumnList,
			s.Prefix,
			updateSet,
			s.Prefix,
			s.Prefix,
			s.Prefix,
		))); err != nil {
			return errors.Errorf("failed to merge into table %s from %s: %w", s.TableName, tempTable, err)
		}

		sdk.Logger(ctx).Info().Msg("ran MERGE for updates/deletes")
	}

	if err := tx.Commit(); err != nil {
		return errors.Errorf("transaction failed: %w", err)
	}

	return nil
}

func toStr(fields []string) string {
	return strings.Join(fields, ", ")
}

func buildQuery(ctx context.Context, query string) string {
	sb := sqlbuilder.Build(query)
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