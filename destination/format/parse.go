// You can edit this code!
// Click here and start typing.
package format

import (
	"context"
	"fmt"

	avro "github.com/hamba/avro/v2"
)

func main() {
	sch := `{"name":"conduit.postgres.account_balance_snapshot_small","type":"record","fields":[{"name":"account_id","type":"int"},{"name":"balance","type":{"name":"conduit.postgres.decimal","type":"fixed","size":38,"logicalType":"decimal","precision":38,"scale":2}},{"name":"created_at","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"date","type":{"type":"int","logicalType":"date"}},{"name":"household_id","type":"int"},{"name":"id","type":"long"},{"name":"updated_at","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

	schema, err := avro.Parse(sch)
	if err != nil {
		panic(err)
	}
	fmt.Printf("schema: %+v\n", schema)

	recordSchema, ok := schema.(*avro.RecordSchema)
	if !ok {
		panic("could not coerce avro schema into recordSchema")
	}

	fmt.Printf("recordschema: %+v\n", recordSchema)

	for _, f := range recordSchema.Fields() {
		fmt.Printf("field: %+v\n", f)
	}
}

func mapAvroToSnowflake2(ctx context.Context, field *avro.Field) string {
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
