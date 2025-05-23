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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return &Source{}
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp opencdc.Position) error {
	it, err := iterator.New(ctx, s.config.Connection, s.config.Table, s.config.OrderingColumn, s.config.Keys,
		s.config.Columns, s.config.BatchSize, s.config.Snapshot, rp)
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}

	s.iterator = it

	return nil
}

// Read gets the next object from the snowflake.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p opencdc.Position) error {
	return s.iterator.Ack(ctx, p)
}
