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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Teardown(t *testing.T) {
		is := is.New(t)
	
		testCases := []struct{
			desc string
			mockWriter func(ctrl *gomock.Controller) *mock.MockWriter
			wantErr error
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
			t.Run(tc.desc, func(t *testing.T){
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

	testCases := []struct{
		desc string
		records []sdk.Record
		mockWriter func(ctrl *gomock.Controller,rr []sdk.Record) *mock.MockWriter
		wantNumRecords int
		wantErr error
	}{
		{
			desc: "success",
			records: []sdk.Record{
				{
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"yeee": "haw",
						},
					},
				},
			},
			mockWriter: func(ctrl *gomock.Controller, rr []sdk.Record) *mock.MockWriter {
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
			records: []sdk.Record{
				{
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"yeee": "haw",
						},
					},
				},
			},
			mockWriter: func(ctrl *gomock.Controller, rr []sdk.Record) *mock.MockWriter {
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
		t.Run(tc.desc, func(t *testing.T){
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
	exampleConfig := map[string]string{
		"snowflake.stage": "orders_stage",
		"snowflake.primaryKey": "id",
		"snowflake.namingPrefix": "meroxa",
		"snowflake.format": "csv",
		"sdk.batch.size": "10",
		"sdk.batch.delay": "1s",
		"snowflake.url": "u:p@localhost:1818/db/schema",
		"snowflake.table": "orders",
	}

	want := Config{
		Config: config.Config{
			Table: "orders",
			Connection: "u:p@localhost:1818/db/schema",
		},
		NamingPrefix: "meroxa",
		PrimaryKey: []string{"id"},
		Stage: "orders_stage",
		Format: "csv",
	}

	is := is.New(t)
	var got Config
	err := sdk.Util.ParseConfig(exampleConfig, &got)

	is.NoErr(err)
	is.Equal(cmp.Diff(want, got), "")
}

func TestDestination_Teardown_NoOpen(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	underTest := NewDestination()
	is.NoErr(underTest.Teardown(ctx))
}
