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

package format

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema/snowflake"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hamba/avro/v2"
)

// TODO: just create the table with the types on the left to make this simpler.

// AvroToSnowflakeType Map from Avro Types to Snowflake Types.
var AvroToSnowflakeType = map[avro.Type]snowflake.DataType{
	avro.Boolean: snowflake.DataTypeBoolean{IsNullable: true},
	avro.Int:     snowflake.DataTypeFixed{IsNullable: true},
	avro.Long:    snowflake.DataTypeFixed{IsNullable: true},
	avro.Float:   snowflake.DataTypeReal{IsNullable: true},
	avro.Double:  snowflake.DataTypeReal{IsNullable: true},
	avro.Bytes:   snowflake.DataTypeText{IsNullable: true},
	avro.String:  snowflake.DataTypeText{IsNullable: true},
	avro.Record:  snowflake.DataTypeObject{IsNullable: true},
	avro.Array:   snowflake.DataTypeArray{IsNullable: true},
	avro.Map:     snowflake.DataTypeObject{IsNullable: true},
}

const (
	AvroBoolean         = "boolean"
	AvroInt             = "int"
	AvroLong            = "long"
	AvroFloat           = "float"
	AvroDouble          = "double"
	AvroBytes           = "bytes"
	AvroString          = "string"
	AvroDecimal         = "decimal"
	AvroUUID            = "uuid"
	AvroDate            = "date"
	AvroTimeMillis      = "time-millis"
	AvroTimeMicros      = "time-micros"
	AvroTimestampMillis = "timestamp-millis"
	AvroTimestampMicros = "timestamp-micros"
	AvroRecord          = "record"
	AvroArray           = "array"
	AvroMap             = "map"
)

type recordSummary struct {
	updatedAt    time.Time
	createdAt    time.Time
	deletedAt    time.Time
	latestRecord sdk.Record
}

type ConnectorColumns struct {
	operationColumn string
	createdAtColumn string
	updatedAtColumn string
	deletedAtColumn string
}

type AvroRecordSchema struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Fields []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"fields"`
}

func GetDataSchema(
	ctx context.Context,
	record sdk.Record,
	prefix string,
) (snowflake.Table, error) {
	var t snowflake.Table

	t.Operation = snowflake.Column{Name: prefix + "_operation", DataType: snowflake.DataTypeText{IsNullable: true}}
	t.CreatedAt = snowflake.Column{Name: prefix + "_created_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}}
	t.UpdatedAt = snowflake.Column{Name: prefix + "_updated_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}}
	t.DeletedAt = snowflake.Column{Name: prefix + "_deleted_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}}

	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this
	// TODO: support schema evolution.

	// TODO: after SDK v0.10.0 use the standard schema metadata fields for this
	avroStr, okAvro := record.Metadata["postgres.avro.schema"]
	// if we have an avro schema in the metadata, interpret the schema from it
	if okAvro {
		sdk.Logger(ctx).Debug().Msgf("avro schema string: %s", avroStr)
		avroSchema, err := avro.Parse(avroStr)
		if err != nil {
			return snowflake.Table{}, fmt.Errorf("could not parse avro schema: %w", err)
		}
		avroRecordSchema, ok := avroSchema.(*avro.RecordSchema)
		if !ok {
			return snowflake.Table{}, errors.New("could not coerce avro schema into recordSchema")
		}

		t.Columns = make([]snowflake.Column, 0, len(avroRecordSchema.Fields()))
		for _, field := range avroRecordSchema.Fields() {
			dt, err := mapAvroToSnowflake(ctx, field.Type())
			if err != nil {
				return snowflake.Table{}, fmt.Errorf("failed to map avro field %s: %w", field.Name(), err)
			}
			t.Columns = append(t.Columns, snowflake.Column{
				Name:     field.Name(),
				DataType: dt,
			})
		}
	} else {
		data, err := extractStructuredPayload(record)
		if err != nil {
			return snowflake.Table{}, fmt.Errorf("failed to extract payload data: %w", err)
		}

		// TODO (BEFORE MERGE): move to function
		t.Columns = make([]snowflake.Column, 0, len(data))
		for key, val := range data {
			var dt snowflake.DataType
			switch val.(type) {
			case int, int8, int16, int32, int64:
				dt = snowflake.DataTypeFixed{IsNullable: true, Precision: 38}
			case float32, float64:
				dt = snowflake.DataTypeReal{IsNullable: true}
			case bool:
				dt = snowflake.DataTypeBoolean{IsNullable: true}
			case time.Time, *time.Time:
				dt = snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}
			case nil:
				// We should keep track of variants separately in case we run into concrete type later on
				// if we ran into none null value of this variant col, we can execute an alter to dest table
				dt = snowflake.DataTypeVariant{IsNullable: true}
			default:
				dt = snowflake.DataTypeText{IsNullable: true}
			}
			t.Columns = append(t.Columns, snowflake.Column{
				Name:     key,
				DataType: dt,
			})
		}
	}

	// detect primary keys by checking the key data
	keyData, err := extractStructuredKey(record)
	if err != nil {
		return snowflake.Table{}, fmt.Errorf("failed to extract key data, can't detect primary keys: %w", err)
	}
	for key := range keyData {
		for _, col := range t.Columns {
			if strings.EqualFold(key, col.Name) {
				t.PrimaryKeys = append(t.PrimaryKeys, col)
				break
			}
		}
	}
	if len(t.PrimaryKeys) == 0 {
		return snowflake.Table{}, errors.New("no primary keys detected")
	}

	// sort data column order alphabetically to make deterministic
	// but keep conduit connector columns at the front for ease of use
	sort.Slice(t.Columns, func(i, j int) bool {
		return t.Columns[i].Name < t.Columns[j].Name
	})

	t.Columns = append(
		[]snowflake.Column{
			t.Operation,
			t.CreatedAt,
			t.UpdatedAt,
			t.DeletedAt,
		},
		t.Columns...,
	)

	sdk.Logger(ctx).Debug().Msgf("schema detected: %+v", t)

	return t, nil
}

// TODO: refactor this function, make it more modular and readable.
func MakeCSVBytes(
	ctx context.Context,
	records []sdk.Record,
	table snowflake.Table,
	buf *bytes.Buffer,
) (err error) {
	writer := csv.NewWriter(buf)

	// loop through records and de-dupe before converting to CSV
	// this is done beforehand, so we can parallelize the CSV formatting
	latestRecordMap := make(map[string]*recordSummary, len(records))
	dedupedRecords := make([]*recordSummary, 0, len(records))
	sdk.Logger(ctx).Debug().Msgf("num of records in batch before deduping: %d", len(records))

	for i, r := range records {
		readAt, err := r.Metadata.GetReadAt()
		if err != nil {
			return err
		}

		s, err := extractStructuredPayload(r)
		key := fmt.Sprint(s[table.PrimaryKeys[0].Name]) // TODO support composite primary keys
		if err != nil {
			return err
		}

		l, ok := latestRecordMap[key]
		if !ok {
			l = &recordSummary{
				latestRecord: r,
			}
			latestRecordMap[key] = l
			dedupedRecords = append(dedupedRecords, l)
		}

		switch r.Operation {
		case sdk.OperationUpdate:
			if readAt.After(l.updatedAt) {
				l.updatedAt = readAt
				l.latestRecord = records[i]
			}
		case sdk.OperationDelete:
			l.deletedAt = readAt
			l.latestRecord = records[i]
		case sdk.OperationCreate, sdk.OperationSnapshot:
			l.createdAt = readAt
			l.latestRecord = records[i]
		}
	}
	clear(latestRecordMap) // not needed anymore

	// Process CSV records
	sdk.Logger(ctx).Debug().Msgf("num of records in batch after deduping: %d", len(dedupedRecords))

	header := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		header[i] = col.Name
	}
	err = writer.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	err = createCSVRecords(
		ctx,
		dedupedRecords,
		writer,
		table,
	)
	if err != nil {
		return fmt.Errorf("failed to create CSV records")
	}

	sdk.Logger(ctx).Debug().Msgf("num rows in CSV buffer: %d", len(dedupedRecords))

	return nil
}

func getColumnValue(
	col snowflake.Column,
	s *recordSummary,
	data sdk.StructuredData,
	table snowflake.Table,
) (string, error) {
	r := s.latestRecord
	switch {
	case col == table.Operation:
		return r.Operation.String(), nil
	case col == table.CreatedAt:
		if r.Operation == sdk.OperationCreate || r.Operation == sdk.OperationSnapshot {
			return strconv.FormatInt(s.createdAt.UnixMicro(), 10), nil
		}
		return "", nil
	case col == table.UpdatedAt:
		if r.Operation == sdk.OperationUpdate {
			return strconv.FormatInt(s.updatedAt.UnixMicro(), 10), nil
		}
		return "", nil
	case col == table.DeletedAt:
		if r.Operation == sdk.OperationDelete {
			return strconv.FormatInt(s.deletedAt.UnixMicro(), 10), nil
		}
		return "", nil
	case data[col.Name] == nil:
		return "", nil
	case isDateOrTimeType(col):
		t, ok := data[col.Name].(time.Time)
		if !ok {
			return "", fmt.Errorf("invalid timestamp on column %s: %+v", col.Name, data[col.Name])
		}
		if _, ok := col.DataType.(snowflake.DataTypeDate); ok {
			return t.UTC().Format(time.DateOnly), nil
		} else {
			return fmt.Sprint(t.UTC().UnixMicro()), nil
		}
	default:
		return fmt.Sprint(data[col.Name]), nil
	}
}

func createCSVRecords(
	_ context.Context,
	recordSummaries []*recordSummary,
	writer *csv.Writer,
	table snowflake.Table,
) error {
	rows := make([][]string, len(recordSummaries))

	for i, s := range recordSummaries {
		row := make([]string, len(table.Columns))

		data, err := extractStructuredPayload(s.latestRecord)
		if err != nil {
			return fmt.Errorf("failed to extract payload data: %w", err)
		}

		for j, col := range table.Columns {
			value, err := getColumnValue(col, s, data, table)
			if err != nil {
				return err
			}
			row[j] = value
		}
		rows[i] = row
	}

	return writer.WriteAll(rows)
}

func extractStructuredPayload(record sdk.Record) (sdk.StructuredData, error) {
	sdkData := record.Payload.After
	if record.Operation == sdk.OperationDelete {
		sdkData = record.Payload.Before
	}
	return extractStructuredData(sdkData)
}

func extractStructuredKey(record sdk.Record) (sdk.StructuredData, error) {
	return extractStructuredData(record.Key)
}

func extractStructuredData(data sdk.Data) (sdk.StructuredData, error) {
	switch data := data.(type) {
	case sdk.StructuredData:
		return data, nil
	case sdk.RawData:
		sd := make(sdk.StructuredData)
		if err := json.Unmarshal(data, &sd); err != nil {
			return nil, fmt.Errorf("cannot unmarshal raw data into structured: %w", err)
		}
		return sd, nil
	default:
		return nil, fmt.Errorf("data payload does not contain structured or raw data (%T)", data)
	}
}

func isDateOrTimeType(col snowflake.Column) bool {
	switch col.DataType.(type) {
	case snowflake.DataTypeDate,
		snowflake.DataTypeTime,
		snowflake.DataTypeTimestampTz,
		snowflake.DataTypeTimestampLtz,
		snowflake.DataTypeTimestampNtz:
		return true
	default:
		return false
	}
}

func mapAvroToSnowflake(ctx context.Context, s avro.Schema) (snowflake.DataType, error) {
	// primitive schema
	switch s := s.(type) {
	case *avro.PrimitiveSchema:
		// check if there's a logical type
		if ls := s.Logical(); ls != nil {
			switch ls.Type() {
			case avro.Decimal:
				return snowflake.DataTypeReal{IsNullable: true}, nil
			case avro.UUID:
				return snowflake.DataTypeText{
					IsNullable: true,
					Length:     36,
					ByteLength: 36,
					Fixed:      false,
				}, nil
			case avro.Date:
				return snowflake.DataTypeDate{IsNullable: true}, nil
			case avro.TimeMillis, avro.TimestampMillis:
				return snowflake.DataTypeTimestampTz{
					IsNullable: true,
					Precision:  0,
					Scale:      3,
				}, nil
			case avro.TimeMicros, avro.TimestampMicros:
				return snowflake.DataTypeTimestampTz{
					IsNullable: true,
					Precision:  0,
					Scale:      6,
				}, nil
			case avro.Duration:
				return snowflake.DataTypeText{
					IsNullable: true,
				}, nil
			}
		}

		// Otherwise, fall back to primitives
		sfType, ok := AvroToSnowflakeType[s.Type()]
		if ok {
			return sfType, nil
		}

	case *avro.FixedSchema:
		if s.Logical().Type() == avro.Decimal {
			sdk.Logger(ctx).Trace().Msg("decimal detected")

			return snowflake.DataTypeFixed{IsNullable: true}, nil
		}
	case *avro.UnionSchema:
		// TODO add support for union schema
	}

	return nil, fmt.Errorf("could not find snowflake mapping for avro type %s", s.Type())
}
