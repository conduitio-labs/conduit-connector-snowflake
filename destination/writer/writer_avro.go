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
	"slices"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	valueSchema = "kafkaconnect.value.schema"
	keySchema   = "kafkaconnect.key.schema"
)

type Avro struct {
	Prefix      string
	PrimaryKey  string
	Stage       string
	TableName   string
	FileThreads int

	db          *sql.DB
	schema      avro.Schema
	schemaTypes map[string]avro.Type
	evolver     *schema.Evolver
	insertBuf   *bytes.Buffer
	updatesBuf  *bytes.Buffer
}

var _ Writer = (*Avro)(nil)

func NewAvro(ctx context.Context, cfg *SnowflakeConfig) (*Avro, error) {
	db, err := sql.Open("snowflake", cfg.Connection)
	if err != nil {
		return nil, errors.Errorf("failed to connect to snowflake db")
	}

	// create the stage if it doesn't exist, replace it if already present
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf("CREATE OR REPLACE STAGE %s", cfg.Stage),
	); err != nil {
		return nil, errors.Errorf("failed to create stage %q: %w", cfg.Stage, err)
	}

	// create basic table
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (%s_metadata VARIANT) ENABLE_SCHEMA_EVOLUTION = true",
			cfg.TableName, cfg.Prefix,
		),
	); err != nil {
		return nil, errors.Errorf("failed to create table %q: %w", cfg.TableName, err)
	}

	return &Avro{
		Prefix:      cfg.Prefix,
		PrimaryKey:  cfg.PrimaryKey,
		Stage:       cfg.Stage,
		TableName:   cfg.TableName,
		FileThreads: cfg.FileThreads,
		db:          db,
		insertBuf:   &bytes.Buffer{},
		updatesBuf:  &bytes.Buffer{},
		evolver:     schema.NewEvolver(db),
	}, nil
}

func (w *Avro) Close(ctx context.Context) error {
	if _, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP STAGE %s", w.Stage)); err != nil {
		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	if err := w.db.Close(); err != nil {
		return errors.Errorf("failed to gracefully close the connection: %w", err)
	}

	return nil
}

func (w *Avro) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// assign request id to the write cycle
	ctx = withRequestID(ctx)

	if w.schema == nil {
		if err := w.initSchema(ctx, records); err != nil {
			return 0, errors.Errorf("failed to initialize schema from records: %w", err)
		}

		migrated, err := w.evolver.Migrate(ctx, w.TableName, w.schema)
		if err != nil {
			return 0, errors.Errorf("failed to evolve schema during boot: %w", err)
		}

		sdk.Logger(ctx).Debug().
			Bool("success", migrated).
			Msg("schema initialized and migration completed")
	}

	var inserts, updates, deletes []*sdk.Record

	defer w.updatesBuf.Reset()
	defer w.insertBuf.Reset()

	// N.B. Prepare records by operation.
	//      Processing first inserts, then updates.
	//      Deletes are staggarred at the end of all updates.
	for i, r := range records {
		switch r.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			inserts = append(inserts, &records[i])
		case sdk.OperationUpdate:
			updates = append(updates, &records[i])
		case sdk.OperationDelete:
			deletes = append(deletes, &records[i])
		}
	}

	sdk.Logger(ctx).Debug().
		Int("inserts", len(inserts)).
		Int("updates", len(updates)).
		Int("deletes", len(deletes)).
		Msg("records prepared")

	// process schema off records, initialize if not set, need a single snapshot/create
	updates = append(updates, deletes...)

	inserted, err := w.insert(ctx, inserts)
	if err != nil {
		return 0, errors.Errorf("failed to insert %d records: %w", len(inserts), err)
	}

	updated, err := w.merge(ctx, updates)
	if err != nil {
		return 0, errors.Errorf("failed to merge %d records: %w", len(updates), err)
	}

	return updated + inserted, nil
}

func (w *Avro) insert(ctx context.Context, records []*sdk.Record) (int, error) {
	encoder, err := ocf.NewEncoder(
		w.schema.String(),
		w.insertBuf,
		ocf.WithCodec(ocf.ZStandard), // options are snappy, zstandard and deflate
	)
	if err != nil {
		return 0, errors.Errorf("failed to initialize avro encoder: %w", err)
	}

	for _, r := range records {
		data, ok := r.Payload.After.(sdk.StructuredData)
		if !ok {
			return 0, errors.Errorf("payload.after (%T) is not structured data", data)
		}

		// N.B. When JSON is serialized all numbers are serialized to doubles.
		//      Ensure correct types as per avro.
		if err := encoder.Encode(coerceTypes(w.schemaTypes, data)); err != nil {
			sdk.Logger(ctx).Debug().
				Msgf("failed to encode data %+v", data)

			return 0, errors.Errorf("failed to encode data: %w", err)
		}
	}

	if err := encoder.Flush(); err != nil {
		sdk.Logger(ctx).Debug().
			Int("encoded", w.insertBuf.Len()).
			Msg("failed to flush encoded data")

		return 0, errors.Errorf("failed to flush encoded data: %w", err)
	}

	stageFile, err := w.upload(ctx, w.insertBuf)
	if err != nil {
		return 0, errors.Errorf("failed to store file %q in stage %q: %w", stageFile, w.Stage, err)
	}

	if _, err := w.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`COPY INTO %s FROM @%s FILES = ('%s')
			 FILE_FORMAT = (TYPE = avro) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PURGE = TRUE`,
			w.TableName, w.Stage, stageFile,
		),
	); err != nil {
		return 0, errors.Errorf(
			"failed to import file %q from stage %q into table %q: %w",
			stageFile, w.Stage, w.TableName, err,
		)
	}

	return len(records), nil
}

func (w *Avro) merge(ctx context.Context, records []*sdk.Record) (int, error) {
	_ = ctx
	_ = records

	return 0, nil
}

// initSchema creates a schema definition from the first record which has the value schema.
func (w *Avro) initSchema(ctx context.Context, records []sdk.Record) error {
	i := slices.IndexFunc(records, func(r sdk.Record) bool {
		return r.Metadata != nil && r.Metadata[valueSchema] != ""
	})

	if i < 0 {
		return errors.Errorf("failed to find record with schema")
	}

	if ks, ok := records[i].Metadata[keySchema]; ok {
		_ = ks // do something with it
	}

	ksch, err := schema.ParseKafkaConnect(records[i].Metadata[valueSchema])
	if err != nil {
		return errors.Errorf("failed to parse kafka schema: %w", err)
	}

	avsc, err := schema.NewAvroSchema(ksch)
	if err != nil {
		return errors.Errorf("failed to construct avro schema: %w", err)
	}

	sdk.Logger(ctx).Debug().Str("schema", avsc.String()).Msg("schema created")

	w.schemaTypes = schema.AvroFields(avsc)
	w.schema = avsc

	return nil
}

func (w *Avro) upload(ctx context.Context, buf *bytes.Buffer) (string, error) {
	ctx = sf.WithFileStream(ctx, buf)
	ctx = sf.WithFileTransferOptions(ctx, &sf.SnowflakeFileTransferOptions{
		RaisePutGetError: true,
	})

	filename := fmt.Sprintf("%s_inserts.avro", requestID(ctx))

	if _, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"PUT file://%s @%s AUTO_COMPRESS=true PARALLEL=%d", filename, w.Stage, w.FileThreads,
	)); err != nil {
		return "", errors.Errorf("failed to upload %q to stage %q: %w", filename, w.Stage, err)
	}

	return filename + ".gz", nil
}

func coerceTypes(t map[string]avro.Type, sd sdk.StructuredData) sdk.StructuredData {
	for k, v := range sd {
		switch v.(type) {
		case float32, float64:
			switch t[k] {
			case avro.Long:
				sd[k] = int64(sd[k].(float64))
			case avro.Int:
				sd[k] = int32(sd[k].(float64))
			default:
				sd[k] = sd[k].(float64)
			}
		}
	}

	return sd
}
