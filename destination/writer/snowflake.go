package writer

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const S3FilesWrittenLength = 100

type Snowflake struct {
	KeyPrefix    string
	Bucket       string
	Position     sdk.Position
	Error        error
	FilesWritten []string
}

var _ Writer = (*Snowflake)(nil)

type SnowflakeConfig struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Bucket          string
	KeyPrefix       string
}

func NewS3(ctx context.Context, cfg *SnowflakeConfig) (*Snowflake, error) {

	return &Snowflake{
		Bucket:       cfg.Bucket,
		KeyPrefix:    cfg.KeyPrefix,
		FilesWritten: make([]string, 0, S3FilesWrittenLength),
	}, nil
}

func (w *Snowflake) Write(ctx context.Context, batch *Batch) error {
	_, _ := batch.Bytes()

	w.FilesWritten = append(w.FilesWritten, "key")

	w.Position = batch.LastPosition()

	return nil
}

// LastPosition returns the last persisted position
func (w *Snowflake) LastPosition() sdk.Position {
	return w.Position
}
