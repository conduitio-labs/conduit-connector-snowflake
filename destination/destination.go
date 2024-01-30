package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/google/uuid"
)

const (
	defaultBatchDelay = time.Second * 5
	defaultBatchSize  = 1000
)

type Destination struct {
	sdk.UnimplementedDestination
	repository *repository.Snowflake
	config     Config
}

// NewDestination creates the Destination and wraps it in the default middleware.
func NewDestination() sdk.Destination {
	// This is needed to override the default batch size and delay defaults for this destination connector.
	middlewares := sdk.DefaultDestinationMiddleware()
	for i, m := range middlewares {
		switch dest := m.(type) {
		case sdk.DestinationWithBatch:
			dest.DefaultBatchDelay = defaultBatchDelay
			dest.DefaultBatchSize = defaultBatchSize
			middlewares[i] = dest
		}
	}
	return sdk.DestinationWithMiddleware(&Destination{}, middlewares...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Destination Connector.")

	// TODO: add configuration for ordering column (aka primary key)
	// right now, we will automatically detect the key from the key within the record,
	// but it would be great to have flexibility in case the user wants to key on a different

	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("failed to parse destination config : %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	var err error
	d.repository, err = repository.Create(ctx, d.config.Connection)
	if err != nil {
		return fmt.Errorf("failed to create snowflake repository: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// TLDR - we don't need to implement custom batching logic, it's already handled
	// for us in the SDK, as long as we use sdk.batch.size / sdk.batch.delay.

	// sdk.batch.size and sdk.batch.delay already handles batching and should
	// control the size of records & timing of when Write() method is invoked.
	// FYI - these are only implemented in the SDK for destinations

	// General approach

	csv, schema, orderingCols, err := format.MakeCSVRecords(records, d.config.NamingPrefix, d.config.PrimaryKey)
	if err != nil {
		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	fmt.Printf(" @@@ CSV DATA  %s \n ", string(csv.Bytes()))

	// ON START OF CONNECTOR:

	// 1. generate internal stage

	fileName := fmt.Sprintf("%s.csv", uuid.NewString())
	fmt.Printf("@@@@ FILE NAME %s \n", fileName)

	if err = d.repository.SetupStage(ctx, d.config.Stage); err != nil {
		return 0, errors.Errorf("failed to set up snowflake stage: %w", err)
	}

	tempTable, err := d.repository.SetupTables(ctx, d.config.Table, schema)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake tables: %w", err)
	}

	fmt.Println("set up tables just fine")

	if err := d.repository.PutFileInStage(ctx, csv, fileName, d.config.Stage); err != nil {
		return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
	}

	fmt.Println("put files in stage")

	if err := d.repository.Copy(ctx, tempTable, d.config.Stage, fileName); err != nil {
		return 0, errors.Errorf("failed copy file to temporary table: %w", err)
	}

	if err := d.repository.Merge(ctx, d.config.Table, tempTable, d.config.NamingPrefix, schema, orderingCols); err != nil {
		return 0, errors.Errorf("failed merge temp to prod : %w", err)
	}

	if err := d.repository.Cleanup(ctx, d.config.Stage, fileName); err != nil {
		return 0, errors.Errorf("failed remove files from stage: %w", err)
	}

	// FOR EACH BATCH:

	// upon receiving first record:

	// 1. intepret the "schema" of the data, and see whether that matches what we've cached

	// 1a. if the cache is empty, then we need to create destination table + create temporary table
	//  create destination table + temp table
	//  CREATE TABLE IF NOT EXISTS "test_data" (
	//        id INT, descr varchar, hello varchar, ajkd varchar, jdlsjd varchar
	//      )
	//  CREATE TEMPORARY TABLE:
	// 		  id INT, descr varchar, hello varchar, ajkd varchar, jdlsjd varchar
	//      );

	// 1b.
	// if the cache is not empty, but is different than the record (e.g. we have a new column),
	// 	 then we need to execute an ALTER TABLE on destination table +
	//  ALTER TABLE ....
	//  CREATE TEMPORARY TABLE:
	// 		  id INT, descr varchar, hello varchar, ajkd varchar, jdlsjd varchar
	//      );

	// 2. Create a CSV containing the batch of records to put into Snowflake

	// write file somewhere locally? then we can

	// 3. Use snowflake's SQL PUT command to upload these files into the internal stage:
	// PUT file:///Users/samir/batch-of-records.csv @conduit_connector:j9j2824;

	// Keep in mind that the file will be compressed with GZIP,
	// so the filename in the stage is "batch-of-records.csv.gz"

	// 4. execute the COPY INTO command to load the contents of the CSV into the temporary table:
	// COPY INTO "temp_data1" FROM @test_stage1 pattern='.*/.*/snowflake1.csv.gz'
	//   FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

	// 5. MERGE the temporary table into the destination table, making sure to handle UPDATES and DELETES:
	// MERGE INTO "test_data" as a USING "temp_data1" AS b ON a.id = b.id
	// WHEN MATCHED AND meroxa_operation = "OPERATION_UPDATE" THEN UPDATE SET a.descr = b.descr, a.hello = b.hello, a.ajkd = b.ajkd, a.jdlsjd = b.jdlsjd, a.meroxa_updated_at = CURRENT_TIMESTAMP()
	// WHEN MATCHED AND meroxa_operation = "OPERATION_DELETE" THEN UPDATE SET a.meroxa_deleted_at = CURRENT_TIMESTAMP();
	// WHEN NOT MATCHED THEN INSERT (a.meroxa_created_at,a.id, a.descr, a.hello, a.ajkd, a.jdlsjd) VALUES (CURRENT_TIMESTAMP(), b.id, b.descr, b.hello, b.ajkd, b.jdlsjd);

	// NOTE: we need to update the query above to actually read the record to find `UPDATE`/`DELETE`, and perform the action as recommended.
	// This was just utilized for testing performance and cost.

	// Now the destination table should be updated with the records from the batch. Let's clean up by:

	// 6. Remove the batch file:
	// REMOVE @test_stage1/batch-of-records.csv.gz

	// delete batch file locally too!

	return 0, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	if d.repository != nil {
		d.repository.Close()
	}

	return nil
}
