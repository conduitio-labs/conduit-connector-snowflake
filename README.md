# Conduit Connector Snowflake

### General

The Snowflake connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides the source
snowflake connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- (optional) [mock](https://github.com/golang/mock) 1.6.0

### Configuration

The config passed to `Configure` can contain the following fields.

| name         | description                                                                                                                                                                                                                                     | required | example                                                |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------|
| `connection` | Snowflake connection string.<br/>Supported formats:<br><code>user:password@my_organization-my_account/dbname/schemaname</code> or <br><code>username[:password]@hostname:port/dbname/schemaname </code><br><b>Important</b>: Schema is required | yes      | "user:password@my_organization-my_account/mydb/schema" |
| `table`      | The table name in snowflake db.                                                                                                                                                                                                                 | yes      | "users"                                                |
| `columns`    | Comma separated list of column names that should be included in each Record's payload. By default: all columns.                                                                                                                                 | no       | "id,name,age"                                          |
| `primaryKey` | Column name that records should use for their `Key` fields.                                                                                                                                                                                     | yes      | "id"                                                   |
| `batchSize`  | Size of batch. By default is 1000. <b>Important:</b> Please don't update this variable, it will cause problem with position.                                                                                                                    | no       | "100"                                                  |

### How to build it

Run `make build`.

### Testing

Run `make test`.

### Snowflake Source

The Snowflake Source Connector connects to a snowflake with the provided configurations, using
`connection`, `table`,`columns`, `primaryKey`, `batchSize`  and using snowflake driver.
Source method `Configure`  parse the configurations.
`Open` method is called to start the connection from the provided position get the
data from snowflake db. The `Read` return next record. The `Ack` method
checks if the record with the position was recorded. The `Teardown` do gracefully shutdown.

#### Snapshot Iterator

The snapshot iterator starts getting data from the table using select query with limit and offset.
Batch size is configurable, offset value is zero for first time. Iterator save rows from table
to `currentBatch` slice variable.

Iterator `HasNext` method check if next element exist in `currentBatch` using variable `index`
and if it is needed change offset and run select query to get new data with new offset. Method `Next` gets
next element and converts it to `Record` sets metadata variable `table`, set metadata variable `action` - `insert`,
increase `index`.

If snapshot stops, it will parse position from last record. Position has fields: `Element` - it is the index of element
in current batch, this last element what was recorded, `Offset` - shows the last value offset what iterator used for
getting data query. Iterator runs query to get data from table with `batchSize` and `offset` which was got from
position. `index` value will be `Element` increased by one, because iterator tries to find next element in current batch.
If `index` > `batchSize` iterator will change `offset` to next and set `index` zero.

For example, we get snapshot position in `Open` function:

```json
{
 "Type" : "s",
 "IndexInBatch": 2,
 "BatchID": 10
}
```

Last recorded position has `BatchID` = 10, it is means iterator did last time query with `offset` value 10, iterator will
do the same query with the  same `offset` value. Iterator gets batch with rows from table. `IndexInBatch` it is last
index for element in this batch what was processed. Iterator looks for next element in batch (with index = 3) and convert
it to record. 

#### CDC Iterator

The CDC iterator starts working if snapshot iterator method `HasNext` return false.
The CDC iterator uses snowflake 'stream' (more information about streams
https://docs.snowflake.com/en/user-guide/streams-intro.html).

The change tracking system utilized by the `stream` then records information about the
DML changes after this snapshot was taken. Change records provide the state
of a row before and after the change. Change information mirrors the column structure
of the tracked source object and includes additional metadata columns that describe each change event.
`Stream` itself does not contain any table data. When we add new row to table after stream creation or stream consuming
this row will be added to stream table. If this row will be removed, this row will be removed from stream table
When row was added to table before stream creation or stream consuming it is not exist in stream table.
If this row will be removed, we will get record about it in stream table.

`Stream` has columns:

`METADATA$ACTION`: Indicates the DML operation (INSERT, DELETE) recorded.<br>
`METADATA$ISUPDATE`: Indicates whether the operation was part of an UPDATE statement.
Updates to rows in the source object are represented as a pair of DELETE and
INSERT records in the stream with a metadata column METADATA$ISUPDATE values set to TRUE.<br>
`METADATA$ROW_ID`: Specifies the unique and immutable ID for the row, which can be used to track changes
to specific rows over time.

When source starts work first time iterator <b>creates</b> stream with name `conduit_stream_{table}` to `table` from
config, <b>creates</b> table for consuming stream with name `conduit_tracking_{table}`.
This consuming table has the same schema as `table`  with additional metadata columns:
`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` (Those columns from stream) and `METADATA$TS`.
`METADATA$TS` it is timestamp column, it is special column created by iterator to ordering rows from tracking table. When
Then iterator consume data from stream using insert query to consuming table. `METADATA$TS` will have  current timestamp value.
After consuming stream, tracking table has copy of stream data with inserted time. All rows from stream were
automatically removed and stream did new snapshot for `table`.


Iterator run select query for getting data from consuming table using limit and offset and ordering by `METADATA$TS`.
Batch size is configurable, offset value is zero for first time.
Iterator save information from table to `currentBatch` slice variable. Iterator `HasNext` method check if next element
exist in `currentBatch` using variable `index` and if it is needed change offset and run select query to get new data
with new offset. Method `Next` gets next element converts it to `Record` checks action(can be `insert`, `delete`, `update`)
using metadata columns `METADATA$ACTION`, `METADATA$ISUPDATE`. Iterator increases `index` and tries to find next record.


For example, we have table with name  `CLIENTS` and fields `ID`, `NAME`. Connector creates
stream with name `CONDUIT_STREAM_CLIENTS` and creates tracking table with name `CONDUIT_TRACKING_CLIENTS` with fields:
`ID`,`NAME`, `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`, `METADATA$TS`. We remove row with id = 2, which
was inserted before stream creation we get row in `CONDUIT_STREAM_CLIENTS`.

| ID     | NAME     | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                            |
|--------|----------|-----------------|-------------------|--------------------------------------------|
| 2      | Test     | DELETE          | FALSE             |    fafe92c9c207a714bfbf8ef55e32c501852b5c8e|

Then we add new client. Stream table will look like:

| ID   | NAME     | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID                           |
|------|----------|-----------------|-------------------|-------------------------------------------|
| 5    | Foo      | INSERT          | FALSE             | ef465fb7a243abcb3ef019b6c5ce89d490218b11  |
| 2    | Test     | DELETE          | FALSE             | fafe92c9c207a714bfbf8ef55e32c501852b5c8e  |

Connector consumes stream running query `INSERT INTO CONDUIT_TRACKING_CLIENTS SELECT *,
current_timestamp() FROM CONDUIT_STREAM_CLIENTS`. After this stream will be empty, we will have data on tracking table.
The connector will run select query:

```sql
SELECT * FROM CONDUIT_TRACKING_CLIENTS ORDER BY METADATA$TS LIMIT {batchSize} OFFSET 0;
```


Connectors will transform this data to records.


<b>NOTE:</b> please pay attention and don't accidentally delete `stream` and tracking table were created by CDC iterator.

#### Position

Position has fields: `Type` (`c` - CDC or `s`- Snapshot), `IndexInBatch`(index of element of current
batch), `BatchID`(offset value).

Example:

```json
{
 "Type" : "c",
 "IndexInBatch": 2,
 "BatchID": 5
}
```
