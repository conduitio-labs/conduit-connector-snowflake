package writer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sf "github.com/snowflakedb/gosnowflake"

	"github.com/go-errors/errors"
	"github.com/google/uuid"
	"github.com/huandu/go-sqlbuilder"
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

// SnowflakeConfig is a type used to initialize an Snowflake Writer
type SnowflakeConfig struct {
	Prefix     string
	PrimaryKey []string
	Stage      string
	TableName  string
	Connection string
	Format     format.Format
}

// NewSnowflake takes an SnowflakeConfig reference and produces an Snowflake Writer
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

func (w *Snowflake) Close() error {
	if err := w.SnowflakeDB.Close(); err != nil {
		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

// Write stores the batch on AWS Snowflake as a file
func (w *Snowflake) Write(ctx context.Context, records []sdk.Record) (int, error) {

	fmt.Println(" @@@@@@@@@ RUNNING WRITER  - ")
	fmt.Printf(" @@@@@ MODE - %s", w.Format)

	var (
		schema     map[string]string
		indexCols  []string
		colOrder   []string
		insertsBuf *bytes.Buffer
		updatesBuf *bytes.Buffer
		err        error
	)

	switch w.Format {
	case "csv":
		//this is csv
		fmt.Println("@@@@ Case CSV")
		insertsBuf, updatesBuf, schema, indexCols, colOrder, err = w.Format.MakeBytes(records, w.Prefix, w.PrimaryKey)
		if err != nil {
			return 0, errors.Errorf("failed to convert records to CSV: %w", err)
		}
	default:
		return 0, errors.Errorf("Mode not implemented.")
	}

	// generate a UUID used for the temporary table and filename in internal stage
	batchUUID := strings.Replace(uuid.NewString(), "-", "", -1)
	insertsFilename := fmt.Sprintf("inserts_%s.csv", batchUUID)
	updatesFilename := fmt.Sprintf("updates_%s.csv", batchUUID)

	tempTable, err := w.SetupTables(ctx, batchUUID, schema)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake tables: %w", err)
	}

	fmt.Printf("@@@@ TABLES ARE SET UP %s \n", updatesFilename)

	fmt.Printf("@@@@ insertsBuf.Len()=%d \n", insertsBuf.Len())
	fmt.Printf("@@@@ updatesBuf.Len()=%d \n", updatesBuf.Len())

	if insertsBuf != nil && insertsBuf.Len() > 0 {
		if err := w.PutFileInStage(ctx, insertsBuf, insertsFilename); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
		fmt.Printf("@@@@ uploaded inserts file to stage %s \n", insertsFilename)

		if err := w.CopyInserts(ctx, insertsFilename, colOrder); err != nil {
			return 0, errors.Errorf("failed copy file to table: %w", err)
		}
		fmt.Println("@@@@ ran COPY INTO for inserts")
	}

	if updatesBuf != nil && updatesBuf.Len() > 0 {
		if err := w.PutFileInStage(ctx, updatesBuf, updatesFilename); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
		fmt.Printf("@@@@ uploaded updates file to stage %s \n", insertsFilename)

		if err := w.CopyUpdates(ctx, tempTable, updatesFilename); err != nil {
			return 0, errors.Errorf("failed copy file to temporary table: %w", err)
		}
		fmt.Println("@@@@ ran COPY INTO for updates")

		if err := w.Merge(ctx, tempTable, indexCols, schema); err != nil {
			return 0, errors.Errorf("failed merge temp to prod : %w", err)
		}
		fmt.Println("@@@@ ran MERGE for updates")
	}

	//w.Position = batch.LastPosition()

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
	queryCreateTemporaryTable := `CREATE TEMPORARY TABLE IF NOT EXISTS %s (%s)`
	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTemporaryTable, tempTable, columnsSQL))); err != nil {
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
	return tx.Commit()
}

func (s *Snowflake) CopyInserts(ctx context.Context, filename string, colOrder []string) error {
	query := ""
	switch s.Format {
	case format.CSV:
		colList := buildFinalColumnList("", "", colOrder)
		aliasFields := make([]string, len(colOrder))
		for i := 0; i < len(colOrder); i++ {
			aliasFields[i] = fmt.Sprintf(`f.$%d`, i+1)
		}
		aliasCols := toStr(aliasFields)
		query = fmt.Sprintf(`
			COPY INTO %s (%s, %s_created_at)
			FROM (
				SELECT %s, CURRENT_TIMESTAMP()
				FROM @%s/%s f
			)
			FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);`,
			s.TableName, colList, s.Prefix, aliasCols, s.Stage, filename,
		)
	default:
		return errors.Errorf("invalid format: %s", s.Format)
	}

	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	if _, err = tx.ExecContext(ctx, query); err != nil {
		return errors.Errorf("failed to copy file %s in %s: %w", filename, s.TableName, err)
	}

	return tx.Commit()
}

func (s *Snowflake) CopyUpdates(ctx context.Context, tempTable, filename string) error {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint
	queryCopyInto := `
		COPY INTO %s FROM @%s
		FILES = ('%s.gz')
		FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  PARSE_HEADER = TRUE)
		MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE' PURGE = TRUE;`

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCopyInto, tempTable, s.Stage, filename))); err != nil {
		return errors.Errorf("failed to copy file %s in temp %s: %w", filename, tempTable, err)
	}

	return tx.Commit()
}

func (s *Snowflake) Merge(ctx context.Context, tempTable string, orderingCols []string, schema map[string]string) error {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	cols := maps.Keys(schema)
	updateSet := buildOrderingColumnList("a", "b", ",", cols)
	orderingColumnList := buildOrderingColumnList("a", "b", " AND ", orderingCols)

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

	return tx.Commit()
}

// GetPrimaryKeys returns all primary keys of the table.
func (s *Snowflake) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	var columns []string

	queryGetPrimaryKeys := `SHOW PRIMARY KEYS IN TABLE %s`

	rows, err := s.SnowflakeDB.QueryContext(ctx, fmt.Sprintf(queryGetPrimaryKeys, table))
	if err != nil {
		return nil, errors.Errorf("query get max value: %w", err)
	}
	defer rows.Close()

	dest := make(map[string]any)
	for rows.Next() {
		if err = rows.Scan(dest); err != nil {
			return nil, errors.Errorf("scan primary key row: %w", err)
		}

		columns = append(columns, dest["column_name"].(string))
	}

	return columns, nil
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
