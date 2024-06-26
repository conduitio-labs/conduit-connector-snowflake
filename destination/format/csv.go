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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hamba/avro/v2"
	"golang.org/x/exp/maps"
)

// TODO: just create the table with the types on the left to make this simpler.

// SnowflakeTypeMapping Map between snowflake retrieved types and connector defined types.
var SnowflakeTypeMapping = map[string]string{
	SnowflakeFixed:        SnowflakeInteger,
	SnowflakeReal:         SnowflakeFloat,
	SnowflakeText:         SnowflakeVarchar,
	SnowflakeBinary:       SnowflakeBinary,
	SnowflakeBoolean:      SnowflakeBoolean,
	SnowflakeDate:         SnowflakeDate,
	SnowflakeTimestampNTZ: SnowflakeTimestampNTZ,
	SnowflakeTimestampLTZ: SnowflakeTimestampLTZ,
	SnowflakeTimestampTZ:  SnowflakeTimestampTZ,
	SnowflakeVariant:      SnowflakeVariant,
	SnowflakeObject:       SnowflakeObject,
	SnowflakeArray:        SnowflakeArray,
	SnowflakeVector:       SnowflakeVector,
}

const (
	// numeric types.
	SnowflakeInteger = "INTEGER"
	SnowflakeFloat   = "FLOAT"
	SnowflakeFixed   = "FIXED"
	SnowflakeReal    = "REAL"

	// string & binary types.
	SnowflakeText    = "TEXT"
	SnowflakeBinary  = "BINARY"
	SnowflakeVarchar = "VARCHAR"

	// logical data types.
	SnowflakeBoolean = "BOOLEAN"

	// date & time types.
	SnowflakeTimestampLTZ = "TIMESTAMP_LTZ"
	SnowflakeTimestampNTZ = "TIMESTAMP_NTZ"
	SnowflakeTimestampTZ  = "TIMESTAMP_TZ"
	SnowflakeTime         = "TIME"
	SnowflakeDate         = "DATE"

	// semi-structured data types.
	SnowflakeVariant = "VARIANT"
	SnowflakeObject  = "OBJECT"
	SnowflakeArray   = "ARRAY"

	// geospatial data types.
	SnowflakeGeography = "GEOGRAPHY"
	SnowflakeGeometry  = "GEOMETRY"

	// vector data types.
	SnowflakeVector = "VECTOR"
)

// AvroToSnowflakeType Map from Avro Types to Snowflake Types.
var AvroToSnowflakeType = map[avro.Type]string{
	avro.Boolean: SnowflakeBoolean,
	avro.Int:     SnowflakeInteger,
	avro.Long:    SnowflakeInteger,
	avro.Float:   SnowflakeFloat,
	avro.Double:  SnowflakeFloat,
	avro.Bytes:   SnowflakeVarchar,
	avro.String:  SnowflakeVarchar,
	avro.Record:  SnowflakeObject,
	avro.Array:   SnowflakeArray,
	avro.Map:     SnowflakeObject,
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
	latestRecord *sdk.Record
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
	schema map[string]string,
	prefix string,
) ([]string, ConnectorColumns, error) {
	// we need to store the operation in a column, to detect updates & deletes
	connectorColumns := ConnectorColumns{
		operationColumn: fmt.Sprintf("%s_operation", prefix),
		createdAtColumn: fmt.Sprintf("%s_created_at", prefix),
		updatedAtColumn: fmt.Sprintf("%s_updated_at", prefix),
		deletedAtColumn: fmt.Sprintf("%s_deleted_at", prefix),
	}

	schema[connectorColumns.operationColumn] = SnowflakeVarchar
	schema[connectorColumns.createdAtColumn] = SnowflakeTimestampTZ
	schema[connectorColumns.updatedAtColumn] = SnowflakeTimestampTZ
	schema[connectorColumns.deletedAtColumn] = SnowflakeTimestampTZ

	csvColumnOrder := []string{}

	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	// Grab the schema from the first record.
	// TODO: support schema evolution.

	data, err := extract(record.Operation, record.Payload, record.Key)
	if err != nil {
		return nil, ConnectorColumns{}, fmt.Errorf("failed to extract payload data: %w", err)
	}

	avroStr, okAvro := record.Metadata["postgres.avro.schema"]
	// if we have an avro schema in the metadata, interpret the schema from it
	if okAvro {
		sdk.Logger(ctx).Debug().Msgf("avro schema string: %s", avroStr)
		avroSchema, err := avro.Parse(avroStr)
		if err != nil {
			return nil, ConnectorColumns{}, fmt.Errorf("could not parse avro schema: %w", err)
		}
		avroRecordSchema, ok := avroSchema.(*avro.RecordSchema)
		if !ok {
			return nil, ConnectorColumns{}, errors.New("could not coerce avro schema into recordSchema")
		}
		for _, field := range avroRecordSchema.Fields() {
			csvColumnOrder = append(csvColumnOrder, field.Name())
			schema[field.Name()], err = mapAvroToSnowflake(ctx, field)
			if err != nil {
				return nil, ConnectorColumns{}, fmt.Errorf("failed to map avro field %s: %w", field.Name(), err)
			}
		}
	} else {
		// TODO (BEFORE MERGE): move to function
		for key, val := range data {
			if schema[key] == "" {
				csvColumnOrder = append(csvColumnOrder, key)
				switch val.(type) {
				case int, int8, int16, int32, int64:
					schema[key] = SnowflakeInteger
				case float32, float64:
					schema[key] = SnowflakeFloat
				case bool:
					schema[key] = SnowflakeBoolean
				case time.Time, *time.Time:
					schema[key] = SnowflakeTimestampTZ
				case nil:
					// WE SHOULD KEEP TRACK OF VARIANTS SEPERATELY IN CASE WE RUN INTO CONCRETE TYPE LATER ON
					// IF WE RAN INTO NONE NULL VALUE OF THIS VARIANT COL, WE CAN EXECUTE AN ALTER TO DEST TABLE
					schema[key] = SnowflakeVariant
				default:
					schema[key] = SnowflakeVarchar
				}
			}
		}
	}

	// sort data column order alphabetically to make deterministic
	// but keep conduit connector columns at the front for ease of use
	sort.Strings(csvColumnOrder)
	csvColumnOrder = append(
		[]string{
			connectorColumns.operationColumn,
			connectorColumns.createdAtColumn,
			connectorColumns.updatedAtColumn,
			connectorColumns.deletedAtColumn,
		},
		csvColumnOrder...,
	)

	sdk.Logger(ctx).Debug().Msgf("schema detected: %+v", schema)

	return csvColumnOrder, connectorColumns, nil
}

// TODO: refactor this function, make it more modular and readable.
func MakeCSVBytes(
	ctx context.Context,
	records []sdk.Record,
	csvColumnOrder []string,
	meroxaColumns ConnectorColumns,
	schema map[string]string,
	primaryKey string,
	buf *bytes.Buffer,
	_ int,
) (err error) {
	writer := csv.NewWriter(buf)

	// loop through records and de-dupe before converting to CSV
	// this is done beforehand, so we can parallelize the CSV formatting
	latestRecordMap := make(map[string]*recordSummary, len(records))
	sdk.Logger(ctx).Debug().Msgf("num of records in batch before deduping: %d", len(records))

	for i, r := range records {
		readAtMicro, err := strconv.ParseInt(r.Metadata["opencdc.readAt"], 10, 64)
		if err != nil {
			return err
		}
		readAt := time.UnixMicro(readAtMicro)

		s, err := extract(r.Operation, r.Payload, r.Key)
		key := fmt.Sprint(s[primaryKey])
		if err != nil {
			return err
		}

		l, ok := latestRecordMap[key]
		if !ok {
			l = &recordSummary{
				latestRecord: &records[i],
			}
			latestRecordMap[key] = l
		}

		switch r.Operation {
		case sdk.OperationUpdate:
			if readAt.After(l.updatedAt) {
				l.updatedAt = readAt
				l.latestRecord = &records[i]
			}
		case sdk.OperationDelete:
			l.deletedAt = readAt
			l.latestRecord = &records[i]
		case sdk.OperationCreate, sdk.OperationSnapshot:
			l.createdAt = readAt
			l.latestRecord = &records[i]
		}
	}

	// Process CSV records

	dedupedRecords := maps.Values(latestRecordMap)
	sdk.Logger(ctx).Debug().Msgf("num of records in batch after deduping: %d", len(dedupedRecords))

	err = writer.Write(csvColumnOrder)
	if err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}
	err = createCSVRecords(
		ctx,
		dedupedRecords,
		writer,
		csvColumnOrder,
		schema,
		meroxaColumns,
	)
	if err != nil {
		return fmt.Errorf("failed to create CSV records")
	}

	sdk.Logger(ctx).Debug().Msgf("num rows in CSV buffer: %d", len(dedupedRecords))

	return nil
}

func getColumnValue(
	c string, r *sdk.Record,
	s *recordSummary,
	data map[string]interface{},
	cnCols ConnectorColumns,
	schema map[string]string) (string, error) {
	switch {
	case c == cnCols.operationColumn:
		return r.Operation.String(), nil
	case c == cnCols.createdAtColumn && (r.Operation == sdk.OperationCreate || r.Operation == sdk.OperationSnapshot):
		return fmt.Sprint(s.createdAt.UnixMicro()), nil
	case c == cnCols.updatedAtColumn && r.Operation == sdk.OperationUpdate:
		return fmt.Sprint(s.updatedAt.UnixMicro()), nil
	case c == cnCols.deletedAtColumn && r.Operation == sdk.OperationDelete:
		return fmt.Sprint(s.deletedAt.UnixMicro()), nil
	case data[c] == nil:
		return "", nil
	case (!isOperationTimestampColumn(c, cnCols)) && isDateOrTimeType(schema[c]):
		t, ok := data[c].(time.Time)
		if !ok {
			return "", fmt.Errorf("invalid timestamp on column %s: %+v", c, data[c])
		}
		if schema[c] == SnowflakeDate {
			return t.UTC().Format(time.DateOnly), nil
		} else {
			return fmt.Sprint(t.UTC().UnixMicro()), nil
		}
	default:
		return fmt.Sprint(data[c]), nil
	}
}

func createCSVRecords(
	_ context.Context,
	recordSummaries []*recordSummary,
	writer *csv.Writer,
	csvColumnOrder []string,
	schema map[string]string,
	cnCols ConnectorColumns,
) error {
	rows := make([][]string, len(recordSummaries))

	for i, s := range recordSummaries {
		if s.latestRecord == nil {
			continue
		}
		r := s.latestRecord

		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload, r.Key)
		if err != nil {
			return fmt.Errorf("failed to extract payload data: %w", err)
		}

		for j, c := range csvColumnOrder {
			value, err := getColumnValue(c, r, s, data, cnCols, schema)
			if err != nil {
				return err
			}
			row[j] = value
		}

		rows[i] = row
	}

	return writer.WriteAll(rows)
}

func extract(op sdk.Operation, payload sdk.Change, key sdk.Data) (sdk.StructuredData, error) {
	var sdkData sdk.Data
	if op == sdk.OperationDelete {
		sdkData = payload.Before
	} else {
		sdkData = payload.After
	}

	dataStruct, okStruct := sdkData.(sdk.StructuredData)
	dataRaw, okRaw := sdkData.(sdk.RawData)

	if !okStruct && !okRaw {
		dataStruct, okStruct = key.(sdk.StructuredData)
		dataRaw, okRaw = key.(sdk.RawData)
		if !okRaw && !okStruct {
			return nil, fmt.Errorf("cannot find data either in payload (%T) or key (%T)", sdkData, key)
		}
		sdkData = key
	}

	if okStruct {
		return dataStruct, nil
	} else if okRaw {
		data := make(sdk.StructuredData)
		if err := json.Unmarshal(dataRaw, &payload); err != nil {
			return nil, fmt.Errorf("cannot unmarshal raw data into structured (%T)", sdkData)
		}

		return data, nil
	}

	return nil, fmt.Errorf("data payload does not contain structured or raw data (%T)", sdkData)
}

// Writes the contents of buffer b to buffer a.
func joinBuffers(a *bytes.Buffer, b *bytes.Buffer) error {
	a.Grow(a.Len())

	if _, err := b.WriteTo(a); err != nil {
		return err
	}

	return nil
}

func isOperationTimestampColumn(col string, cnCols ConnectorColumns) bool {
	switch col {
	case cnCols.operationColumn, cnCols.createdAtColumn, cnCols.updatedAtColumn, cnCols.deletedAtColumn:
		return true
	default:
		return false
	}
}

func isDateOrTimeType(in string) bool {
	switch in {
	case SnowflakeTimestampLTZ, SnowflakeTimestampNTZ, SnowflakeTimestampTZ, SnowflakeDate, SnowflakeTime:
		return true
	default:
		return false
	}
}

func mapAvroToSnowflake(ctx context.Context, field *avro.Field) (string, error) {
	t := field.Type()

	// primitive schema
	p, ok := t.(*avro.PrimitiveSchema)
	if ok {
		// check if there's a logical type
		ls := p.Logical()
		if ls != nil {
			switch ls.Type() {
			case avro.Decimal:
				return SnowflakeFloat, nil
			case avro.UUID:
				return SnowflakeVarchar, nil
			case avro.Date:
				return SnowflakeDate, nil
			case avro.TimeMillis:
				return SnowflakeTimestampTZ, nil
			case avro.TimeMicros:
				return SnowflakeTimestampTZ, nil
			case avro.TimestampMillis:
				return SnowflakeTimestampTZ, nil
			case avro.TimestampMicros:
				return SnowflakeTimestampTZ, nil
			}
		}

		// Otherwise, fall back to primitives
		sfType, ok := AvroToSnowflakeType[t.Type()]
		if ok {
			return sfType, nil
		}
	}

	// fixed types
	f, ok := t.(*avro.FixedSchema)
	if ok && f.Logical().Type() == avro.Decimal {
		sdk.Logger(ctx).Trace().Msg("decimal detected")

		return SnowflakeFloat, nil
	}

	return "", fmt.Errorf("could not find snowflake mapping for avro type %s", field.Name())
}
