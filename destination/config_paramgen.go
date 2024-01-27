// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package destination

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (DestinationConfig) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"snowflake.table": {
			Default:     "",
			Description: "snowflake.table name.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"snowflake.url": {
			Default:     "",
			Description: "snowflake.url string connection to snowflake DB. Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-snowflake.url_String",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"stageName": {
			Default:     "",
			Description: "stageName allows you to name the internal stage created in Snowflake. Not filling this field will result in an auto-generated stage name.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
