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
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/hamba/avro/v2"
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
	latestRecord *opencdc.Record
}

type ConnectorColumns struct {
	OperationColumn string
	CreatedAtColumn string
	UpdatedAtColumn string
	DeletedAtColumn string
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
	records []opencdc.Record,
	schema map[string]string,
	prefix string,
) ([]string, *ConnectorColumns, error) {
	// we need to store the operation in a column, to detect updates & deletes
	connectorColumns := ConnectorColumns{
		OperationColumn: fmt.Sprintf("%s_operation", prefix),
		CreatedAtColumn: fmt.Sprintf("%s_created_at", prefix),
		UpdatedAtColumn: fmt.Sprintf("%s_updated_at", prefix),
		DeletedAtColumn: fmt.Sprintf("%s_deleted_at", prefix),
	}

	schema[connectorColumns.OperationColumn] = SnowflakeVarchar
	schema[connectorColumns.CreatedAtColumn] = SnowflakeTimestampTZ
	schema[connectorColumns.UpdatedAtColumn] = SnowflakeTimestampTZ
	schema[connectorColumns.DeletedAtColumn] = SnowflakeTimestampTZ

	csvColumnOrder := []string{}

	// TODO: see whether we need to support a compound key here
	// TODO: what if the key field changes? e.g. from `id` to `name`? we need to think about this

	// Grab the schema from the first record.
	// TODO: support schema evolution.
	if len(records) == 0 {
		return nil, nil, errors.New("unexpected empty slice of records")
	}

	r := records[0]
	data, err := extractPayload(r.Operation, r.Payload)
	if err != nil {
		return nil, nil, errors.Errorf("failed to extract payload data: %w", err)
	}

	avroStr, okAvro := r.Metadata["postgres.avro.schema"]
	// if we have an avro schema in the metadata, interpret the schema from it
	if okAvro {
		sdk.Logger(ctx).Debug().Msgf("avro schema string: %s", avroStr)
		avroSchema, err := avro.Parse(avroStr)
		if err != nil {
			return nil, nil, errors.Errorf("could not parse avro schema: %w", err)
		}
		avroRecordSchema, ok := avroSchema.(*avro.RecordSchema)
		if !ok {
			return nil, nil, errors.New("could not coerce avro schema into recordSchema")
		}
		for _, field := range avroRecordSchema.Fields() {
			csvColumnOrder = append(csvColumnOrder, field.Name())
			schema[field.Name()], err = mapAvroToSnowflake(ctx, field)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to map avro field %s: %w", field.Name(), err)
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
			connectorColumns.OperationColumn,
			connectorColumns.CreatedAtColumn,
			connectorColumns.UpdatedAtColumn,
			connectorColumns.DeletedAtColumn,
		},
		csvColumnOrder...,
	)

	sdk.Logger(ctx).Debug().Msgf("schema detected: %+v", schema)

	return csvColumnOrder, &connectorColumns, nil
}

// TODO: refactor this function, make it more modular and readable.
func MakeCSVBytes(
	ctx context.Context,
	records []opencdc.Record,
	csvColumnOrder []string,
	meroxaColumns ConnectorColumns,
	schema map[string]string,
	primaryKey string,
	insertsBuf *bytes.Buffer,
	updatesBuf *bytes.Buffer,
	_ int,
) (err error) {
	insertsWriter := csv.NewWriter(insertsBuf)
	updatesWriter := csv.NewWriter(updatesBuf)

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

		k, err := extractKey(r.Key)
		key := fmt.Sprint(k[primaryKey])
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
		case opencdc.OperationUpdate:
			if readAt.After(l.updatedAt) {
				l.updatedAt = readAt
				l.latestRecord = &records[i]
			}
		case opencdc.OperationDelete:
			l.deletedAt = readAt
			l.latestRecord = &records[i]
		case opencdc.OperationCreate, opencdc.OperationSnapshot:
			l.createdAt = readAt
			l.latestRecord = &records[i]
		}
	}

	// Process CSV records
	sdk.Logger(ctx).Debug().Msgf("num of records in batch after deduping: %d", len(latestRecordMap))

	insertsBuffer := new(bytes.Buffer)
	updatesBuffer := new(bytes.Buffer)

	insertW := csv.NewWriter(insertsBuffer)
	updateW := csv.NewWriter(updatesBuffer)

	inserts, updates, err := createCSVRecords(ctx,
		latestRecordMap,
		insertW,
		updateW,
		csvColumnOrder,
		primaryKey,
		schema,
		meroxaColumns)
	if err != nil {
		return errors.Errorf("failed to create CSV records: %w", err)
	}

	sdk.Logger(ctx).Debug().Msgf("num inserts in CSV buffer: %d", inserts)
	sdk.Logger(ctx).Debug().Msgf("num updates/deletes in CSV buffer: %d", updates)

	if inserts > 0 {
		if err := insertsWriter.Write(csvColumnOrder); err != nil {
			return errors.Errorf("failed to write insert headers: %w", err)
		}

		if err := joinBuffers(insertsBuf, insertsBuffer); err != nil {
			return errors.Errorf("failed to join insert buffers: %w", err)
		}
	}

	if updates > 0 {
		if err := updatesWriter.Write(csvColumnOrder); err != nil {
			return errors.Errorf("failed to write update headers: %w", err)
		}

		if err := joinBuffers(updatesBuf, updatesBuffer); err != nil {
			return errors.Errorf("failed to join update buffers: %w", err)
		}
	}

	return nil
}

func getColumnValue(
	c string, r *opencdc.Record,
	s *recordSummary,
	key string,
	primaryKey string,
	data map[string]interface{},
	cnCols ConnectorColumns,
	schema map[string]string,
) (string, error) {
	switch {
	case c == primaryKey:
		return key, nil
	case c == cnCols.OperationColumn:
		return r.Operation.String(), nil
	case c == cnCols.CreatedAtColumn && (r.Operation == opencdc.OperationCreate || r.Operation == opencdc.OperationSnapshot): //nolint:lll //refactor
		return fmt.Sprint(s.createdAt.UnixMicro()), nil
	case c == cnCols.UpdatedAtColumn && r.Operation == opencdc.OperationUpdate:
		return fmt.Sprint(s.updatedAt.UnixMicro()), nil
	case c == cnCols.DeletedAtColumn && r.Operation == opencdc.OperationDelete:
		return fmt.Sprint(s.deletedAt.UnixMicro()), nil
	case r.Operation == opencdc.OperationDelete && c != cnCols.DeletedAtColumn && c != cnCols.OperationColumn:
		// for deletes, pass empty string for everything that isn't primary key, operation, or meroxa_deleted_at.
		// those are the only fields we use for deletes.
		return "", nil
	case data[c] == nil:
		return "", nil
	case (!isOperationTimestampColumn(c, cnCols)) && isDateOrTimeType(schema[c]):
		t, ok := data[c].(time.Time)
		if !ok {
			return "", errors.Errorf("invalid timestamp on column %s: %+v", c, data[c])
		}
		if schema[c] == SnowflakeDate {
			return t.UTC().Format(time.DateOnly), nil
		}

		return fmt.Sprint(t.UTC().UnixMicro()), nil
	default:
		return fmt.Sprint(data[c]), nil
	}
}

func createCSVRecords(
	_ context.Context,
	recordSummaries map[string]*recordSummary,
	insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string,
	primaryKey string,
	schema map[string]string,
	cnCols ConnectorColumns,
) (numInserts int, numUpdates int, err error) {
	var inserts, updates [][]string

	for key, s := range recordSummaries {
		if s.latestRecord == nil {
			continue
		}
		r := s.latestRecord

		row := make([]string, len(csvColumnOrder))

		data, err := extractPayload(r.Operation, r.Payload)
		if err != nil {
			return 0, 0, errors.Errorf("failed to extract payload data: %w", err)
		}

		for j, c := range csvColumnOrder {
			value, err := getColumnValue(c, r, s, key, primaryKey, data, cnCols, schema)
			if err != nil {
				return 0, 0, err
			}
			row[j] = value
		}

		switch r.Operation {
		case opencdc.OperationCreate, opencdc.OperationSnapshot:
			inserts = append(inserts, row)
		case opencdc.OperationUpdate, opencdc.OperationDelete:
			updates = append(updates, row)
		default:
			return 0, 0, errors.Errorf("unexpected opencdc.Operation: %s", r.Operation.String())
		}
	}

	if len(inserts) > 0 {
		if err := insertsWriter.WriteAll(inserts); err != nil {
			return 0, 0, errors.Errorf("failed to write insert records: %w", err)
		}
	}

	if len(updates) > 0 {
		if err := updatesWriter.WriteAll(updates); err != nil {
			return 0, 0, errors.Errorf("failed to write update records: %w", err)
		}
	}

	return len(inserts), len(updates), nil
}

func extractPayload(op opencdc.Operation, payload opencdc.Change) (opencdc.StructuredData, error) {
	var sdkData opencdc.Data
	if op == opencdc.OperationDelete {
		sdkData = payload.Before
	} else {
		sdkData = payload.After
	}

	dataStruct, okStruct := sdkData.(opencdc.StructuredData)
	dataRaw, okRaw := sdkData.(opencdc.RawData)

	if okStruct {
		return dataStruct, nil
	} else if okRaw {
		data := make(opencdc.StructuredData)
		if err := json.Unmarshal(dataRaw, &payload); err != nil {
			return nil, errors.Errorf("cannot unmarshal raw data payload into structured (%T): %w", sdkData, err)
		}

		return data, nil
	}

	return nil, errors.Errorf("cannot find data in payload (%T)", sdkData)
}

func extractKey(key opencdc.Data) (opencdc.StructuredData, error) {
	keyStruct, okStruct := key.(opencdc.StructuredData)
	keyRaw, okRaw := key.(opencdc.RawData)
	if !okRaw && !okStruct {
		return nil, errors.Errorf("cannot find data either in key (%T)", key)
	}

	if okStruct {
		return keyStruct, nil
	} else if okRaw {
		data := make(opencdc.StructuredData)
		if err := json.Unmarshal(keyRaw, &key); err != nil {
			return nil, errors.Errorf("cannot unmarshal raw data key into structured (%T): %w", key, err)
		}

		return data, nil
	}

	return nil, errors.Errorf("cannot find data in key (%T)", key)
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
	case cnCols.OperationColumn, cnCols.CreatedAtColumn, cnCols.UpdatedAtColumn, cnCols.DeletedAtColumn:
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
