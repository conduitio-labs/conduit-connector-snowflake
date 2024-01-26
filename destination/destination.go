package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	repository *repository.Snowflake
	config     DestinationConfig
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	config.Config
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return map[string]sdk.Parameter{
		config.KeyConnection: {
			Default:     "",
			Required:    true,
			Description: "Snowflake connection string.",
		},
		config.KeyTable: {
			Default:     "",
			Required:    true,
			Description: "The table name that the connector should read.",
		},
		config.KeyOrderingColumn: {
			Default:  "",
			Required: true,
			Description: "The name of a column that the connector will use for ordering rows. " +
				"Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.",
		},
		config.KeyColumns: {
			Default:     "",
			Required:    false,
			Description: "Comma separated list of column names that should be included in each Record's payload.",
		},
		config.KeyPrimaryKeys: {
			Default:     "",
			Required:    false,
			Description: "The list of the column names that records should use for their `Key` fields.",
		},
		config.KeySnapshot: {
			Default:  "true",
			Required: false,
			Description: "Whether or not the plugin will take a snapshot of the entire table before starting cdc " +
				"mode, by default true.",
		},
		config.KeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of batch",
		},
	}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Destination...")

	parseConfig, err := config.Parse(cfg)
	if err != nil {
		return err
	}

	d.config.Config = parseConfig

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
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).


	// General approach

	// We should take the following config values
	
	// max batch record count
	// max batch size (bytes)?
	// batch time interval

	//
	// if either the max batch record count or size is reached, we will need to push the batch records into snowflake
	// otherwise, we will default to the time interval to push batches to ensure steady movement of data.

	

	// general approach
	// TODO: move this into another package.


	// ON START OF CONNECTOR:

	// 1. generate internal stage
	// we should try to do this by prepending something like `CONDUIT_`, and then appending the connector ID afterwards.
	// e.g: CREATE STAGE IF NOT EXISTS conduit_connector:j9j2824;

	// 2. create destination table
	// e.g: CREATE TABLE IF NOT EXISTS "test_data" (
	//        id INT, descr varchar, hello varchar, ajkd varchar, jdlsjd varchar
	//      )
	

	// FOR EACH BATCH:

	// 1. create temporary table:
	// CREATE TABLE "temporary_data_g167gd92h" (
	// 		id INT, descr varchar, hello varchar, ajkd varchar, jdlsjd varchar
	// );


	// 2. Create a CSV containing the batch of records to put into Snowflake

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
	
	//	6. Drop the temporary table:
	//  DROP table "temp_data1";

	// 7. Remove the batch file:
	// REMOVE @test_stage1/batch-of-records.csv.gz

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
