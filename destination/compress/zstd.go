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

package compress

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

var _ Compressor = (*Zstd)(nil)

type Zstd struct{}

func (Zstd) Compress(in io.Reader) io.Reader {
	pr, pw := io.Pipe()

	w, err := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		// This would only happen if EncoderLevel is invalid.
		panic(fmt.Errorf("failed to create zstd writer: %w", err))
	}

	go func() {
		defer pw.Close()
		defer w.Close()

		_, err := io.Copy(w, in)
		pw.CloseWithError(err)
	}()

	return pr
}

func (Zstd) Name() string {
	return TypeZstd
}
