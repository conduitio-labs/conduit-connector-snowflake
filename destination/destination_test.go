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

package destination

import (
	"context"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/writer"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/writer/mock"
	sdkconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Teardown(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		desc       string
		mockWriter func(ctrl *gomock.Controller) *mock.MockWriter
		wantErr    error
	}{
		{
			desc: "success",
			mockWriter: func(ctrl *gomock.Controller) *mock.MockWriter {
				w := mock.NewMockWriter(ctrl)
				w.EXPECT().
					Close(gomock.Any()).
					Times(1).
					Return(nil)

				return w
			},
		},
		{
			desc: "close error",
			mockWriter: func(ctrl *gomock.Controller) *mock.MockWriter {
				w := mock.NewMockWriter(ctrl)
				w.EXPECT().
					Close(gomock.Any()).
					Times(1).
					Return(errors.New("test error"))

				return w
			},
			wantErr: errors.New("test error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctx := context.Background()

			var mockWriter writer.Writer
			if tc.mockWriter != nil {
				mockWriter = tc.mockWriter(ctrl)
			}

			d := Destination{
				Config: Config{},
				Writer: mockWriter,
			}

			err := d.Teardown(ctx)
			if tc.wantErr != nil {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tc.wantErr.Error()))
			} else {
				is.NoErr(err)
			}
		})
	}
}

func TestDestination_Write(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		desc           string
		records        []opencdc.Record
		mockWriter     func(ctrl *gomock.Controller, rr []opencdc.Record) *mock.MockWriter
		wantNumRecords int
		wantErr        error
	}{
		{
			desc: "success",
			records: []opencdc.Record{
				{
					Operation: opencdc.OperationCreate,
					Payload: opencdc.Change{
						After: opencdc.StructuredData{
							"yeee": "haw",
						},
					},
				},
			},
			mockWriter: func(ctrl *gomock.Controller, rr []opencdc.Record) *mock.MockWriter {
				w := mock.NewMockWriter(ctrl)
				w.EXPECT().
					Write(gomock.Any(), rr).
					Times(1).
					Return(1, nil)

				return w
			},
			wantNumRecords: 1,
		},
		{
			desc: "write error",
			records: []opencdc.Record{
				{
					Operation: opencdc.OperationCreate,
					Payload: opencdc.Change{
						After: opencdc.StructuredData{
							"yeee": "haw",
						},
					},
				},
			},
			mockWriter: func(ctrl *gomock.Controller, rr []opencdc.Record) *mock.MockWriter {
				w := mock.NewMockWriter(ctrl)
				w.EXPECT().
					Write(gomock.Any(), rr).
					Times(1).
					Return(0, errors.New("test error"))

				return w
			},
			wantErr: errors.New("test error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctx := context.Background()

			var mockWriter writer.Writer
			if tc.mockWriter != nil {
				mockWriter = tc.mockWriter(ctrl, tc.records)
			}

			d := Destination{
				Config: Config{},
				Writer: mockWriter,
			}

			recordsProcessed, err := d.Write(ctx, tc.records)
			if tc.wantErr != nil {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tc.wantErr.Error()))
			} else {
				is.NoErr(err)
				is.Equal(recordsProcessed, tc.wantNumRecords)
			}
		})
	}
}

func Test_ParseConfig(t *testing.T) {
	exampleConfig := sdkconfig.Config{
		"snowflake.stage":        "orders_stage",
		"snowflake.primaryKey":   "id",
		"snowflake.username":     "u",
		"snowflake.password":     "p",
		"snowflake.host":         "localhost",
		"snowflake.port":         "1818",
		"snowflake.database":     "db",
		"snowflake.schema":       "schema",
		"snowflake.warehouse":    "testWarehouse",
		"snowflake.namingPrefix": "meroxa",
		"snowflake.format":       "csv",
		"opencdc.batch.size":     "10",
		"opencdc.batch.delay":    "1s",
		"snowflake.url":          "u:p@localhost:1818/db/schema",
		"snowflake.table":        "orders",
	}

	want := Config{
		Config: config.Config{
			Table: "orders",
		},
		Username:     "u",
		Password:     "p",
		Host:         "localhost",
		Port:         1818,
		Database:     "db",
		Schema:       "schema",
		Warehouse:    "testWarehouse",
		NamingPrefix: "meroxa",
		PrimaryKey:   "id",
		Stage:        "orders_stage",
		Format:       "csv",
	}

	is := is.New(t)
	var got config.Config
	err := sdk.Util.ParseConfig(context.Background(), exampleConfig, &got, NewDestination().Parameters())

	is.NoErr(err)
	is.Equal(cmp.Diff(want, got), "")
}

func TestDestination_Teardown_NoOpen(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	underTest := NewDestination()
	is.NoErr(underTest.Teardown(ctx))
}
