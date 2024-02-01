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
		return nil, fmt.Errorf("failed to connect to snowflake db")
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

// Write stores the batch on AWS Snowflake as a file
func (w *Snowflake) Write(ctx context.Context, records []sdk.Record) (int, error) {

	fmt.Println(" @@@@@@@@@ RUNNING WRITER  - ")
	fmt.Printf(" @@@@@ MODE - %s", w.Format)

	var (
		schema       map[string]string
		orderingCols []string
		insertsBuf   *bytes.Buffer
		updatesBuf   *bytes.Buffer
		err          error
	)

	switch w.Format {
	case "json":
		//this is json
	case "csv":
		//this is csv
		fmt.Println("@@@@ Case CSV")
		insertsBuf, updatesBuf, schema, orderingCols, err = w.Format.MakeBytes(records, w.Prefix, w.PrimaryKey)
		if err != nil {
			return 0, errors.Errorf("failed to convert records to CSV: %w", err)
		}
	default:
		return 0, errors.Errorf("Mode not implemented.")
	}

	fmt.Printf(" @@@ CSV DATA INSERTS  %s \n ", string(insertsBuf.Bytes()))
	fmt.Printf(" @@@ CSV DATA UPDATES/DELETES  %s \n ", string(updatesBuf.Bytes()))

	// generate a UUID used for the temporary table and filename in internal stage
	batchUUID := strings.Replace(uuid.NewString(), "-", "", -1)
	insertsFileName := fmt.Sprintf("inserts_%s.csv", batchUUID)
	updatesFileName := fmt.Sprintf("updates_%s.csv", batchUUID)
	fmt.Printf("@@@@ INSERTS FILE NAME %s \n", insertsFileName)
	fmt.Printf("@@@@ UPDATES FILE NAME %s \n", updatesFileName)

	tempTable, err := w.SetupTables(ctx, batchUUID, schema)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake tables: %w", err)
	}

	if insertsBuf.Len() != 0 {
		if err := w.PutFileInStage(ctx, insertsBuf, insertsFileName); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}
		if err := w.Copy(ctx, w.TableName, insertsFileName); err != nil {
			return 0, errors.Errorf("failed copy file to temporary table: %w", err)
		}
	}

	if updatesBuf.Len() != 0 {
		if err := w.PutFileInStage(ctx, updatesBuf, updatesFileName); err != nil {
			return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
		}

		if err := w.Copy(ctx, tempTable, updatesFileName); err != nil {
			return 0, errors.Errorf("failed copy file to temporary table: %w", err)
		}

		if err := w.Merge(ctx, tempTable, orderingCols, schema); err != nil {
			return 0, errors.Errorf("failed merge temp to prod : %w", err)
		}
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
		return "", fmt.Errorf("create temporary table: %w", err)
	}

	queryCreateTable := `CREATE TABLE IF NOT EXISTS %s (
		%s, 
		meroxa_deleted_at TIMESTAMP_LTZ,
		meroxa_created_at TIMESTAMP_LTZ,
		meroxa_updated_at TIMESTAMP_LTZ )`

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCreateTable, s.TableName, columnsSQL))); err != nil {
		return "", fmt.Errorf("create destination table: %w", err)
	}

	return tempTable, tx.Commit()
}

func (s *Snowflake) PutFileInStage(ctx context.Context, buf *bytes.Buffer, fileName string) error {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	// nolint:errcheck,nolintlint
	queryPutFileInStage := `PUT file://%s @%s parallel=30;` // TODO: make parallelism configurable.

	if _, err = tx.ExecContext(sf.WithFileStream(ctx, buf), buildQuery(ctx, fmt.Sprintf(queryPutFileInStage, fileName, s.Stage))); err != nil {
		return fmt.Errorf("PUT file %s in stage %s: %w", fileName, s.Stage, err)
	}
	return tx.Commit()
}

func (s *Snowflake) Copy(ctx context.Context, tempTable, fileName string) error {
	tx, err := s.SnowflakeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint
	queryCopyInto := `COPY INTO %s FROM @%s
					FILES = ('%s.gz') 
					FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  PARSE_HEADER = TRUE) 
					MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE PURGE = TRUE';`

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryCopyInto, tempTable, s.Stage, fileName))); err != nil {
		return fmt.Errorf("failed to copy file %s in temp %s: %w", fileName, tempTable, err)
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
	insertColumnList := buildFinalColumnList("a", ".", cols)
	valuesColumnList := buildFinalColumnList("b", ".", cols)

	queryMergeInto := `MERGE INTO %s as a USING %s AS b ON %s
		WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s, a.%s_updated_at = CURRENT_TIMESTAMP()
	    WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET a.%s_deleted_at = CURRENT_TIMESTAMP()
		WHEN NOT MATCHED THEN INSERT (%s , a.%s_created_at) VALUES (%s, CURRENT_TIMESTAMP())` // remove created_at from this

	if _, err = tx.ExecContext(ctx, buildQuery(ctx, fmt.Sprintf(queryMergeInto,
		s.TableName,
		tempTable,
		orderingColumnList,
		s.Prefix,
		updateSet,
		s.Prefix,
		s.Prefix,
		s.Prefix,
		insertColumnList,
		s.Prefix,
		valuesColumnList),
	)); err != nil {
		return fmt.Errorf("failed to merge into table %s from %s: %w", s.TableName, tempTable, err)
	}

	return tx.Commit()
}

// GetPrimaryKeys returns all primary keys of the table.
func (s *Snowflake) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	var columns []string

	queryGetPrimaryKeys := `SHOW PRIMARY KEYS IN TABLE %s`

	rows, err := s.SnowflakeDB.QueryContext(ctx, fmt.Sprintf(queryGetPrimaryKeys, table))
	if err != nil {
		return nil, fmt.Errorf("query get max value: %w", err)
	}
	defer rows.Close()

	dest := make(map[string]any)
	for rows.Next() {
		if err = rows.Scan(dest); err != nil {
			return nil, fmt.Errorf("scan primary key row: %w", err)
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
