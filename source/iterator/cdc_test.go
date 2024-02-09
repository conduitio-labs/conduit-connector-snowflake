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

package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
)

func TestCDCIterator_HasNext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)

		i := NewCDCIterator(rp, "test", []string{"ID"}, nil, 0, 0, 10, res)

		hasNext, err := i.HasNext(ctx)
		if err != nil {
			t.Errorf("has next error = \"%s\"", err.Error())
		}

		if !hasNext {
			t.Errorf("got = %v, want %v", hasNext, true)
		}
	})

	t.Run("success_return_false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)
		rp.EXPECT().GetTrackingData(ctx, "conduit_stream_test", "conduit_tracking_test",
			nil, 0, 10).Return(nil, nil)

		i := NewCDCIterator(rp, "test", []string{"ID"}, nil, 2, 0, 10, res)

		hasNext, err := i.HasNext(ctx)
		if err != nil {
			t.Errorf("has next error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(hasNext, false) {
			t.Errorf("got = %v, want %v", hasNext, false)
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)
		rp.EXPECT().GetTrackingData(ctx, "conduit_stream_test", "conduit_tracking_test",
			nil, 0, 10).Return(nil, errors.New("some error"))

		i := NewCDCIterator(rp, "test", []string{"ID"}, nil, 2, 0, 10, res)

		_, err := i.HasNext(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestCDCIterator_Next(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var (
			rawData sdk.RawData
			key     sdk.StructuredData
		)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rawData, _ = json.Marshal(map[string]interface{}{"ID": "2", "NAME": "foo"})
		key = map[string]interface{}{"ID": "2"}
		change := sdk.Change{
			Before: nil,
			After:  rawData,
		}

		rp := mock.NewMockRepository(ctrl)

		i := NewCDCIterator(rp, "test", []string{"ID"}, nil, 0, 0, 10, res)

		rec, err := i.Next(ctx)
		if err != nil {
			t.Errorf("has next error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(rec.Payload, change) {
			t.Errorf("got = %v, want %v", rec.Payload, change)
		}

		if !reflect.DeepEqual(rec.Key, key) {
			t.Errorf("got = %v, want %v", rec.Key, key)
		}
	})
	t.Run("success next record", func(t *testing.T) {
		var (
			rawData sdk.RawData
			key     sdk.StructuredData
		)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rawData, _ = json.Marshal(map[string]interface{}{"ID": "2", "NAME": "foo"})
		key = map[string]interface{}{"ID": "2"}
		change := sdk.Change{
			Before: nil,
			After:  rawData,
		}

		rp := mock.NewMockRepository(ctrl)

		i := NewCDCIterator(rp, "test", []string{"ID"}, nil, 0, 0, 10, res)

		rec, err := i.Next(ctx)
		if err != nil {
			t.Errorf("has next error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(rec.Payload, change) {
			t.Errorf("got = %v, want %v", rec.Payload, change)
		}

		if !reflect.DeepEqual(rec.Key, key) {
			t.Errorf("got = %v, want %v", rec.Key, key)
		}
	})
	t.Run("error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)

		i := NewCDCIterator(rp, "test", []string{"missing_key"}, nil, 0, 0, 10, res)

		_, err := i.Next(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestCDCIterator_Stop(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)
		rp.EXPECT().Close().Return(nil)

		i := NewCDCIterator(rp, "test", []string{"missing_key"}, nil, 0, 0, 10, res)

		err := i.Stop()
		if err != nil {
			t.Errorf("stop \"%s\"", err.Error())
		}
	})
	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		res := []map[string]interface{}{
			{"ID": "2", "NAME": "foo", "METADATA$ACTION": "INSERT", "METADATA$ISUPDATE": false},
			{"ID": "1", "NAME": "bar", "METADATA$ACTION": "DELETE", "METADATA$ISUPDATE": false},
		}

		rp := mock.NewMockRepository(ctrl)
		rp.EXPECT().Close().Return(errors.New("some error"))

		i := NewCDCIterator(rp, "test", []string{"missing_key"}, nil, 0, 0, 10, res)

		err := i.Stop()
		if err == nil {
			t.Errorf("want error")
		}
	})
}
