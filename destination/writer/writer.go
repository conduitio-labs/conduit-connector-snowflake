package writer

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer is an interface that is responsible for persisting record that Destination
// has accumulated in its buffers. The default writer the Destination would use is
// S3Writer, others exists to test local behavior.
type Writer interface {
	Write(context.Context, *Batch) error
	LastPosition() sdk.Position
}
