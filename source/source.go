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

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/config"
	"github.com/conduitio/conduit-connector-snowflake/repository"
	"github.com/conduitio/conduit-connector-snowflake/source/iterator"
	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		return err
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	var index int

	p, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse sdk position: %v", err)
	}

	if p.Element != 0 {
		index = p.Element + 1
	}

	snowflake, err := repository.Create(ctx, s.config.Connection)
	if err != nil {
		return fmt.Errorf("create snowflake repository: %v", err)
	}

	data, err := snowflake.GetData(ctx, s.config.Table, s.config.Columns, p.Offset, s.config.Limit)
	if err != nil {
		return fmt.Errorf("get data: %v", err)
	}

	s.iterator = iterator.New(snowflake, p, s.config.Table,
		s.config.Columns, s.config.Key, index, p.Offset, s.config.Limit, data)

	return nil
}

// Read gets the next object from the snowflake.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %v", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}

		s.iterator = nil
	}

	return nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}
