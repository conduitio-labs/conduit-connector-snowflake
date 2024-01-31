package source

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TODO - Finish test
func TestParseConfig(t *testing.T) {
	exampleConfig := map[string]string{
		KeySnapshot: "true",
	}

	want := Config{
		Config: config.Config{
			Connection: "",
			Table:      "",
		},
		Columns:        nil,
		Keys:           nil,
		OrderingColumn: "",
		BatchSize:      0,
		Snapshot:       true,
	}

	is := is.New(t)
	var got Config
	err := sdk.Util.ParseConfig(exampleConfig, &got)

	is.NoErr(err)
	is.Equal(want, got)
}
