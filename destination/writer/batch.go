package writer

import (
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Batch describes the data that needs to be saved by the Writer
type Batch struct {
	Format  format.Format
	Records []sdk.Record
}

// Bytes returns a byte representation for the Writer to write into a file.
func (b *Batch) Bytes() ([]byte, error) {
	return b.Format.MakeBytes(b.Records)
}

// LastPosition returns the position of the last record in the batch.
func (b *Batch) LastPosition() sdk.Position {
	if len(b.Records) == 0 {
		return nil
	}

	last := b.Records[len(b.Records)-1]
	return last.Position
}
