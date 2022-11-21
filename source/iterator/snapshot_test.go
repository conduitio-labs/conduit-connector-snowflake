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
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator/mock"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

var (
	table          = "test"
	orderingColumn = "id"
	key            = "id"
	batchSize      = 1000
	maxValue       = 12
	pos            = &position.Position{
		IteratorType:             position.TypeSnapshot,
		SnapshotLastProcessedVal: 12,
		SnapshotMaxValue:         12,
	}
)

func TestSnapshotIterator_HasNext(t *testing.T) {
	t.Run("success_nil_position", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		rp := mock.NewMockRepository(ctrl)

		rp.EXPECT().GetMaxValue(ctx, table, orderingColumn).Return(maxValue, nil)

		i, err := newSnapshotIterator(ctx, rp, table, orderingColumn, key, nil, batchSize, nil)
		if err != nil {
			t.Fatal(err)
		}

		rp.EXPECT().GetRows(ctx, table, orderingColumn, nil, nil, 12, batchSize).Return(nil, nil)
		hasNext, err := i.HasNext(ctx)
		if err != nil {
			t.Errorf("has next error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(hasNext, false) {
			t.Errorf("got = %v, want %v", hasNext, false)
		}
	})

	t.Run("success_with_position", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		rp := mock.NewMockRepository(ctrl)

		i, err := newSnapshotIterator(ctx, rp, table, orderingColumn, key, nil, batchSize, pos)
		if err != nil {
			t.Fatal(err)
		}

		rp.EXPECT().GetRows(ctx, table, orderingColumn, nil, pos, 12, batchSize).Return(nil, nil)

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

		rp := mock.NewMockRepository(ctrl)

		i, err := newSnapshotIterator(ctx, rp, table, orderingColumn, key, nil, batchSize, pos)
		if err != nil {
			t.Fatal(err)
		}

		rp.EXPECT().GetRows(ctx, table, orderingColumn, nil, pos, maxValue, batchSize).
			Return(nil, errors.New("error"))

		hasNext, err := i.HasNext(ctx)
		if err == nil {
			t.Errorf("want error")
		}

		if !reflect.DeepEqual(hasNext, false) {
			t.Errorf("got = %v, want %v", hasNext, false)
		}
	})
}

func TestIterator_Stop(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		rp := mock.NewMockRepository(ctrl)

		rp.EXPECT().Close().Return(nil)

		i, err := newSnapshotIterator(ctx, rp, table, orderingColumn, key, nil, batchSize, pos)
		if err != nil {
			t.Fatal(err)
		}

		err = i.Stop()
		if err != nil {
			t.Errorf("stop \"%s\"", err.Error())
		}
	})
	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		rp := mock.NewMockRepository(ctrl)

		rp.EXPECT().Close().Return(errors.New("some error"))

		i, err := newSnapshotIterator(ctx, rp, table, orderingColumn, key, nil, batchSize, pos)
		if err != nil {
			t.Fatal(err)
		}

		err = i.Stop()
		if err == nil {
			t.Errorf("want error")
		}
	})
}
