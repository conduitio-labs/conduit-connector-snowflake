package format

import (
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Format defines the format the data will be persisted in by Destination
type Format string

const (
	// Parquet data format https://parquet.apache.org/
	Parquet Format = "parquet"

	// JSON format
	JSON Format = "json"
)

// All is a variable containing all supported format for enumeration
var All = []Format{
	Parquet,
	JSON,
}

// Parse takes a string and returns a corresponding format or an error
func Parse(name string) (Format, error) {
	switch name {
	case "parquet":
		return Parquet, nil
	case "json":
		return JSON, nil
	default:
		return "", fmt.Errorf("unsupported format: %q", name)
	}
}

// Ext returns a preferable file extension for the given format
func (f Format) Ext() string {
	switch f {
	case Parquet:
		return "parquet"
	case JSON:
		return "json"
	default:
		return "bin"
	}
}

// MimeType returns MIME type (IANA media type or Content-Type) for the format
func (f Format) MimeType() string {
	switch f {
	case JSON:
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

// MakeBytes returns a slice of bytes representing records in a given format
func (f Format) MakeBytes(records []sdk.Record) ([]byte, error) {
	switch f {
	case Parquet:
		return makeParquetBytes(records)
	case JSON:
		return makeJSONBytes(records)
	default:
		return nil, fmt.Errorf("unsupported format: %s", f)
	}
}
