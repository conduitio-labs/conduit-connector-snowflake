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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/hamba/avro/v2"
	"golang.org/x/exp/maps"
)

// Map between snowflake retrieved types and connector defined types
// TODO: just create the table with the types on the left to make this simpler.
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
	// numeric types
	SnowflakeInteger = "INTEGER"
	SnowflakeFloat   = "FLOAT"
	SnowflakeFixed   = "FIXED"
	SnowflakeReal    = "REAL"

	// string & binary types
	SnowflakeText    = "TEXT"
	SnowflakeBinary  = "BINARY"
	SnowflakeVarchar = "VARCHAR"

	// logical data types
	SnowflakeBoolean = "BOOLEAN"

	// date & time types
	SnowflakeTimestampLTZ = "TIMESTAMP_LTZ"
	SnowflakeTimestampNTZ = "TIMESTAMP_NTZ"
	SnowflakeTimestampTZ  = "TIMESTAMP_TZ"
	SnowflakeTime         = "TIME"
	SnowflakeDate         = "DATE"

	// semi-structured data types
	SnowflakeVariant = "VARIANT"
	SnowflakeObject  = "OBJECT"
	SnowflakeArray   = "ARRAY"

	// geospatial data types
	SnowflakeGeography = "GEOGRAPHY"
	SnowflakeGeometry  = "GEOMETRY"

	// vector data types
	SnowflakeVector = "VECTOR"
)

// Map from Avro Types to Snowflake Types
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
	records []sdk.Record,
	schema map[string]string,
	prefix string,
) ([]string, *ConnectorColumns, error) {
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
	if len(records) == 0 {
		return nil, nil, errors.New("unexpected empty slice of records")
	}

	r := records[0]
	data, err := extract(r.Operation, r.Payload, r.Key)
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
			schema[field.Name()] = mapAvroToSnowflake(field)
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

	return csvColumnOrder, &connectorColumns, nil
}

//nolint:gocyclo // TODO: refactor this function, make it more modular and readable.
func MakeCSVBytes(
	ctx context.Context,
	records []sdk.Record,
	csvColumnOrder []string,
	meroxaColumns ConnectorColumns,
	schema map[string]string,
	primaryKey string,
	insertsBuf *bytes.Buffer,
	updatesBuf *bytes.Buffer,
	numGoroutines int,
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

	insertsBuffer := new(bytes.Buffer)
	updatesBuffer := new(bytes.Buffer)

	insertW := csv.NewWriter(insertsBuffer)
	updateW := csv.NewWriter(updatesBuffer)

	inserts, updates, err := createCSVRecords(ctx,
		dedupedRecords,
		insertW,
		updateW,
		csvColumnOrder,
		schema,
		meroxaColumns)
	if err != nil {
		sdk.Logger(ctx).Err(err).Msg("failed to create CSV records")
		return err
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

func createCSVRecords(
	_ context.Context,
	recordSummaries []*recordSummary,
	insertsWriter, updatesWriter *csv.Writer,
	csvColumnOrder []string,
	schema map[string]string,
	cnCols ConnectorColumns,
) (numInserts int, numUpdates int, err error) {
	var inserts, updates [][]string

	for _, s := range recordSummaries {
		if s.latestRecord == nil {
			continue
		}
		r := s.latestRecord

		row := make([]string, len(csvColumnOrder))

		data, err := extract(r.Operation, r.Payload, r.Key)
		if err != nil {
			return 0, 0, errors.Errorf("failed to extract payload data: %w", err)
		}

		for j, c := range csvColumnOrder {
			switch {
			case c == cnCols.operationColumn:
				row[j] = r.Operation.String()
			case c == cnCols.createdAtColumn && (r.Operation == sdk.OperationCreate || r.Operation == sdk.OperationSnapshot):
				row[j] = fmt.Sprint(s.createdAt.UnixMicro())
			case c == cnCols.updatedAtColumn && r.Operation == sdk.OperationUpdate:
				row[j] = fmt.Sprint(s.updatedAt.UnixMicro())
			case c == cnCols.deletedAtColumn && r.Operation == sdk.OperationDelete:
				row[j] = fmt.Sprint(s.deletedAt.UnixMicro())
			case data[c] == nil:
				row[j] = ""
			// Handle timestamps & dates
			// TODO: streamline this, this is getting messy
			case (!isOperationTimestampColumn(c, cnCols)) && isDateOrTimeType(schema[c]):
				t, ok := data[c].(time.Time)
				if !ok {
					return 0, 0, errors.Errorf("invalid timestamp on column %s: %+v", c, data[c])
				}

				if schema[c] == SnowflakeDate {
					row[j] = t.UTC().Format(time.DateOnly)
				} else {
					row[j] = fmt.Sprint(t.UTC().UnixMicro())
				}
			default:
				row[j] = fmt.Sprint(data[c])
			}
		}

		switch r.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			inserts = append(inserts, row)
		case sdk.OperationUpdate, sdk.OperationDelete:
			updates = append(updates, row)
		default:
			return 0, 0, errors.Errorf("unexpected sdk.Operation: %s", r.Operation.String())
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
			return nil, errors.Errorf("cannot find data either in payload (%T) or key (%T)", sdkData, key)
		}
		sdkData = key
	}

	if okStruct {
		return dataStruct, nil
	} else if okRaw {
		data := make(sdk.StructuredData)
		if err := json.Unmarshal(dataRaw, &payload); err != nil {
			return nil, errors.Errorf("cannot unmarshal raw data into structured (%T)", sdkData)
		}

		return data, nil
	}

	return nil, errors.Errorf("data payload does not contain structured or raw data (%T)", sdkData)
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

func mapAvroToSnowflake(field *avro.Field) string {
	p := field.Type().(*avro.PrimitiveSchema)
	switch p.Logical().Type() {
	case avro.Decimal:
		return SnowflakeFloat
	case avro.UUID:
		return SnowflakeVarchar
	case avro.Date:
		return SnowflakeDate
	case avro.TimeMillis:
		return SnowflakeTimestampTZ
	case avro.TimeMicros:
		return SnowflakeTimestampTZ
	case avro.TimestampMillis:
		return SnowflakeTimestampTZ
	case avro.TimestampMicros:
		return SnowflakeTimestampTZ
	}
	return AvroToSnowflakeType[field.Type().Type()]
}
