// Copyright © 2022 Meroxa, Inc.
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

package source

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/conduitio/conduit-connector-snowflake/source/mock"
)

func TestSource_Configure(t *testing.T) {
	s := Source{}

	tests := []struct {
		name        string
		cfg         map[string]string
		wantErr     bool
		expectedErr error
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				"connection": "user:password@my_organization-my_account/mydb",
				"table":      "customer",
				"key":        "id",
			},
			expectedErr: nil,
		},
		{
			name: "missing connection",
			cfg: map[string]string{
				"table":   "customer",
				"columns": "",
				"key":     "id",
			},
			expectedErr: errors.New(`"connection" config value must be set`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Configure(context.Background(), tt.cfg)
			if !reflect.DeepEqual(err, tt.expectedErr) {
				t.Errorf("got = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(sdk.StructuredData)
		st["key"] = "value"

		record := sdk.Record{
			Position:  sdk.Position("1.0"),
			Metadata:  nil,
			CreatedAt: time.Time{},
			Key:       st,
			Payload:   st,
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("run query: failed"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(nil)

		s := Source{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
