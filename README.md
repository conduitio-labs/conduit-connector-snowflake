# Conduit Connector Snowflake

### General

The Snowflake connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides the source
snowflake connector.

### Prerequisites

- [Go](https://go.dev/) 1.21
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.55.2
- (optional) [mock](https://github.com/golang/mock) 1.6.0

### How to build it

Run `make build`.

### Testing

Run `make test`.

## Source
The source connector gets data from the given table in Snowflake, it first starts with taking a
[snapshot](#snapshot-iterator) of the table, then starts capturing [CDC](#cdc-iterator) actions happening on that table.

### Configuration

The config passed to `Configure` can contain the following fields.

| name             | description                                                                                                                                                                                                                                                                                                                                                                                                                                                            | required | example                                                                                                 |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------------------------------------------------------------------------------|
| `connection`     | Snowflake connection string, check the [gosnowflake docs](https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String:~:text=The%20Go%20Snowflake%20Driver%20supports,account_identifier%3E%5B%26param1%3Dvalue%26...%26paramN%3DvalueN%5D) for more details on supported syntaxes. For getting the `<account-identifier>` check [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier).  **Important:** Schema is required. | true     | `user:password@my_organization-my_account/dbname/schema` or `user:password@hostname:port/dbname/schema` |
| `table`          | The table the connector will read records from.                                                                                                                                                                                                                                                                                                                                                                                                                        | true     | "users"                                                                                                 |
| `orderingColumn` | The column name the connector will use for ordering rows. **Important:** Values must be unique and suitable for sorting. Otherwise, the snapshot will not work.                                                                                                                                                                                                                                                                                                        | true     | "id"                                                                                                    |
| `columns`        | Comma-separated list of column names that should be included in each record payload. Default is `false` which translates to `All columns`.                                                                                                                                                                                                                                                                                                                             | false    | "id,name,age"                                                                                           |
| `primaryKeys`    | Comma-separated list of column that records should use for their `Key` fields.                                                                                                                                                                                                                                                                                                                                                                                         | false    | "id,name"                                                                                               |
| `snapshot`       | Whether or not the plugin will take a snapshot of the entire table before starting CDC mode. Default is `true`. Other possible value is `false`.                                                                                                                                                                                                                                                                                                                       | false    | "false"                                                                                                 |
| `batchSize`      | Size of batch. **Important:** Please don't update this variable after the pipeline starts, it will cause problems with tracking position. Default is `1000`.                                                                                                                                                                                                                                                                                                           | false    | "1000"                                                                                                  |


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

### Destination Configurations

| name                | description                                                                                                                                                                                                                                     | required | example                                                            |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------|
| `username`          | Snowflake username.                                                                                                                                                                                                                             | true     | "username"                                                         |
| `password`          | Snowflake password.                                                                                                                                                                                                                             | true     | "password"                                                         |
| `host`              | Snowflake host.                                                                                                                                                                                                                                 | true     | "https://mycompany.us-west-2.snowflakecomputing.com"               |
| `database`          | Snowflake database.                                                                                                                                                                                                                             | true     | "userdb"                                                           |
| `schema`            | Snowflake schema.                                                                                                                                                                                                                               | true     | "STREAM_DATA"                                                      |
| `warehouse`         | Snowflake warehouse.                                                                                                                                                                                                                            | true     | "COMPUTE_WH"                                                       |
| `stage`             | Snowflake stage to use for uploading files before merging into destination table.                                                                                                                                                               | true     | "ordersStage"                                                      |
| `primaryKey`        | Primary key of the source data.                                                                                                                                                                                                                 | true     | "id"                                                               |
| `namingPrefix`      | Prefix to append to `updated_at` , `deleted_at`, `created_at` in destination table. Default is `meroxa_`, translates to `meroxa_updated_at` for update timestamps.                                                                              | false    | "meroxa"                                                           |
| `format`            | Data type of file we upload and copy data from to Snowflake. Default is `csv` and cannot be changed until additional formats are supported.                                                                                                     | true     | "csv"                                                              |
| `compression`       | Compression to use when staging files in Snowflake. Default is `zstd`. Other possible values are `gzip` and `copy`.                                                                                                                             | false    | "zstd"                                                             |
| `sdk.batch.size`    | Maximum size of batch before it gets written to Snowflake. Default is `1000`.                                                                                                                                                                   | false    | "1000"                                                             |
| `sdk.batch.delay`   | Maximum delay before an incomplete batch is written to the destination.                                                                                                                                                                         | false    | 5s                                                                 |
| `csvGoRoutines`     | For CSV processing, the number of goroutines to concurrently process CSV rows. Default is `1`.                                                                                                                                                  | false    | 1                                                                  |
| `fileUploadThreads` | Number of threads to run for PUT file uploads. Default is `30`.                                                                                                                                                                                 | false    | 30                                                                 |
| `keepAlive`         | Whether to keep the session alive even when the connection is idle. Default is `true`.                                                                                                                                                          | false    | true                                                               |
