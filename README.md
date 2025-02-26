# Conduit Connector Snowflake

<!-- readmegen:description -->
The Snowflake connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides the source snowflake connector.

## Source

The source connector gets data from the given table in Snowflake, it first starts with taking a
[snapshot](#snapshot-iterator) of the table, then starts capturing [CDC](#cdc-iterator) actions happening on that table.

### Snapshot Iterator

When the connector first starts, snapshot mode is enabled.

A "snapshot" is the state of a table data at a particular point in time when connector starts work. All changes after this
(delete, update, insert operations) will be captured by the Change Data Capture (CDC) iterator.

First time when the snapshot iterator starts work, it would get the max value from `orderingColumn` and save it to the position.
The snapshot iterator reads all rows, where `orderingColumn` values are less or equal to `maxValue`, in batches,
via `SELECT` with fetching and ordering by `orderingColumn`.

`OrderingColumn` value must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.
The iterator saves the last processed value from `orderingColumn` to the field `SnapshotLastProcessedVal`.
If the snapshot stops, it will parse the position from the last record and will try and get the row
where `{{orderingColumn}} > {{position.SnapshotLastProcessedVal}}`.

When all records are returned, the connector switches to the CDC iterator.

This behavior is enabled by default, but can be turned off by adding `"snapshot":"false"` to the Source configuration,
which will result in the connector skipping the snapshot, and immediately starting with CDC instead.

### CDC Iterator

The CDC iterator starts working if snapshot iterator method `HasNext` return false
(which means that the snapshot is done, or was skipped).
The CDC iterator uses snowflake 'stream' (more information about streams
https://docs.snowflake.com/en/user-guide/streams-intro.html).

The change tracking system utilized by the `stream` then records information about the
CDC changes after this snapshot was taken. "Change records" provide the state
of a row before and after the change. Change information mirrors the column structure
of the tracked source object and includes additional metadata columns that describe each change event.
`Stream` itself does not contain any table data. When we add new rows to the table after the stream creation or consuming,
this row will be added to the stream table. If the row is removed, then it will be removed from the stream table.
If row was added to the table before the stream creation or consuming, then the row will not exist in the stream table.
If this row will be removed, we will get record about it in stream table.

`Stream` has the columns:

* `METADATA$ACTION`: Indicates the CDC operation (INSERT, DELETE) recorded.
* `METADATA$ISUPDATE`: Indicates whether the operation was part of an UPDATE statement.
  Updates to rows in the source object are represented as a pair of DELETE and
  INSERT records in the stream with a metadata column `METADATA$ISUPDATE` value set to TRUE.
* `METADATA$ROW_ID`: Specifies the unique and immutable ID for the row, which can be used to track changes
  to specific rows over time.


When the source starts working for the first time, the iterator creates:
* A stream with the name `conduit_stream_{table}` for the `table` from configurations.
* A tracking table with the name `conduit_tracking_{table}`, used for consuming the stream, and insuring the
  ability to resume the CDC iterator progress after interrupting.

The tracking table has the same schema as `table`  with additional metadata columns:
`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` (columns from the stream) and `METADATA$TS`.
`METADATA$TS` for the timestamp column, which is a special column created by the iterator to insure the ordering from the tracking table.
When the CDC iterator consumes data from the stream, and inserts it into the tracking table. `METADATA$TS` will have the current timestamp value.
So after consuming the stream, the tracking table will have a copy of the stream data with the inserted time.

Iterator run select query for getting data from consuming table using limit and offset and ordering by `METADATA$TS`.
Batch size is configurable, offset value is zero for first time.
Iterator save information from table to `currentBatch` slice variable. Iterator `HasNext` method check if next element
exist in `currentBatch` using variable `index` and if it is needed change offset and run select query to get new data
with new offset. Method `Next` gets next element converts it to `Record` checks action(can be `insert`, `delete`, `update`)
using metadata columns `METADATA$ACTION`, `METADATA$ISUPDATE`. Iterator increases `index` and tries to find next record.


For example, we have the table `CLIENTS` with fields `ID`, `NAME`. The connector creates
stream with name `CONDUIT_STREAM_CLIENTS` and creates a tracking table with name `CONDUIT_TRACKING_CLIENTS` with fields:
`ID`,`NAME`, `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`, `METADATA$TS`. We remove row with id = 2, which
was inserted before stream creation we get row in `CONDUIT_STREAM_CLIENTS`.

| ID | NAME | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                          |
|----|------|-----------------|-------------------|------------------------------------------|
| 2  | Test | DELETE          | FALSE             | fafe92c9c207a714bfbf8ef55e32c501852b5c8e |

Then we add new client. Stream table will look like:

| ID   | NAME     | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                           |
|------|----------|-----------------|-------------------|-------------------------------------------|
| 5    | Foo      | INSERT          | FALSE             | ef465fb7a243abcb3ef019b6c5ce89d490218b11  |
| 2    | Test     | DELETE          | FALSE             | fafe92c9c207a714bfbf8ef55e32c501852b5c8e  |

Connector consumes stream running query `INSERT INTO CONDUIT_TRACKING_CLIENTS SELECT *,
current_timestamp() FROM CONDUIT_STREAM_CLIENTS`. The stream will be empty after this, and we will have the data on
the tracking table. After that, The connector will run select query:

```sql
SELECT * FROM CONDUIT_TRACKING_CLIENTS ORDER BY METADATA$TS LIMIT {batchSize} OFFSET 0;
```

Connectors will transform this data to records.

**NOTE:** DO NOT delete the stream and the tracking table created by the connector.

## Destination

The Snowflake Destination is still in early stages of development - please use with caution as we are still improving it for initial release.
<!-- /readmegen:description -->

### Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "snowflake"
        settings:
          # Table name.
          # Type: string
          # Required: yes
          snowflake.table: ""
          # Connection string connection to snowflake DB. Detail information
          # https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
          # Type: string
          # Required: yes
          snowflake.url: ""
          # BatchSize - size of batch.
          # Type: int
          # Required: no
          snowflake.batchsize: "100"
          # Snapshot whether the plugin will take a snapshot of the entire table
          # before starting cdc.
          # Type: string
          # Required: no
          snowflake.columns: "false"
          # OrderingColumn is a name of a column that the connector will use for
          # ordering rows.
          # Type: string
          # Required: no
          snowflake.orderingColumn: ""
          # Primary keys
          # Type: string
          # Required: no
          snowflake.primaryKeys: ""
          # Snapshot
          # Type: bool
          # Required: no
          snowflake.snapshot: "false"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

### Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "snowflake"
        settings:
          # Compression to use when staging files in Snowflake
          # Type: string
          # Required: yes
          snowflake.compression: "zstd"
          # Database for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.database: ""
          # Data type of file we upload and copy data from to snowflake
          # Type: string
          # Required: yes
          snowflake.format: "csv"
          # Host for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.host: ""
          # Prefix to append to update_at , deleted_at, create_at at destination
          # table
          # Type: string
          # Required: yes
          snowflake.namingPrefix: "meroxa"
          # Password for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.password: ""
          # Port for the snowflake connection
          # Type: int
          # Required: yes
          snowflake.port: "0"
          # Primary key of the source table
          # Type: string
          # Required: yes
          snowflake.primaryKey: ""
          # Schema for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.schema: ""
          # Snowflake Stage to use for uploading files before merging into
          # destination table.
          # Type: string
          # Required: yes
          snowflake.stage: ""
          # Table name.
          # Type: string
          # Required: yes
          snowflake.table: ""
          # Username for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.username: ""
          # Warehouse for the snowflake connection
          # Type: string
          # Required: yes
          snowflake.warehouse: ""
          # Automatically clean uploaded files to stage after processing, except
          # when they fail.
          # Type: bool
          # Required: no
          snowflake.autoCleanupStage: "true"
          # Number of threads to run for PUT file uploads.
          # Type: int
          # Required: no
          snowflake.fileUploadThreads: "30"
          # Whether to keep the session alive even when the connection is idle.
          # Type: bool
          # Required: no
          snowflake.keepAlive: "true"
          # For CSV processing, the number of goroutines to concurrently process
          # CSV rows.
          # Type: int
          # Required: no
          snowflake.processingWorkers: "1"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

### How to build it

Run `make build`.

### Testing

Run `make test`.

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=e4563fe6-7b7e-412e-8022-d7b768d21c19)
