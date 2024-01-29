package destination

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
)

type Destination struct {
	sdk.UnimplementedDestination
	sdk.DestinationWithBatch
	repository *repository.Snowflake
	config     Config
}

const (
	tempDir = "/temp/conduit-snowflake"
)

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Destination Connector.")
	d.DefaultBatchSize = 1000
	d.DefaultBatchDelay = time.Second * 5

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

	// TODO: move this into another package.

	csv, schema, cols, err := format.MakeCSVRecords(records)
	if err != nil {
		return 0, errors.Errorf("failed to convert records to CSV: %w", err)
	}

	// TODO (BEFORE MERGING): write CSV file, so we can execute PUT correctly.
	file, err := os.CreateTemp(os.TempDir(), "snowflake_*.csv")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(" @@@ CSV DATA  %s \n ", string(csv))

	if _, err := file.Write(csv); err != nil {
		return 0, errors.Errorf("failed to write csv %w", err)
	}

	// ON START OF CONNECTOR:

	// 1. generate internal stage
	// we should try to do this by prepending something like `CONDUIT_`, and then appending the connector ID afterwards.
	// TODO: see if we can grab connector ID from the connector SDK.
	// e.g: CREATE STAGE IF NOT EXISTS conduit_connector:j9j2824;

	fullFilePath := file.Name()
	fileName := filepath.Base(file.Name())
	fmt.Printf("@@@@ FILE NAME %s \n", fileName)
	fmt.Printf("@@@@ FILE PATH %s \n", fullFilePath)

	tempTable, err := d.repository.SetupDestination(ctx, d.config.Stage, d.config.Table, schema)
	if err != nil {
		return 0, errors.Errorf("failed to set up snowflake destination: %w", err)
	}

	if err := d.repository.PutFileInStage(ctx, fullFilePath, d.config.Stage); err != nil {
		return 0, errors.Errorf("failed put CSV file to snowflake stage: %w", err)
	}

	if err := d.repository.CopyMergeDrop(ctx, d.config.Table, tempTable, d.config.Stage, fileName, schema, cols); err != nil {
		return 0, errors.Errorf("failed copy and merge: %w", err)
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
	// WHEN MATCHED THEN UPDATE SET a.descr = b.descr, a.hello = b.hello, a.ajkd = b.ajkd, a.jdlsjd = b.jdlsjd
	// WHEN NOT MATCHED THEN INSERT (a.id, a.descr, a.hello, a.ajkd, a.jdlsjd) VALUES (b.id, b.descr, b.hello, b.ajkd, b.jdlsjd);

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
