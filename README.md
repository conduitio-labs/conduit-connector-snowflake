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
Batch size is configurable, offset value is zero for first time. Iterator save information from table 
to `data` slice variable. <br>
Iterator `HasNext` method check if next element exist in `data` using variable `index`
and if it is needed change offset and run select query to get new data with new offset. Method `Next` gets 
next element and converts it to `Record` sets metadata variable `table`, set metadata variable `action` - `insert`,
increase `index`.<br>
If snapshot stops, it will parse position from last record. Position has fields: `Element` - it is the index of element 
in current batch, this last element what was recorded, `Offset` - shows the last value offset what iterator used in get data
query. Iterator runs query to get data from table with `batchSize` and `offset` which was got from position. 
`index` value will be `Element` increased by one, because iterator tries to find next element in current batch.
If `index` > `batchSize` iterator will change `offset` to next and set `index` zero. 

#### CDC Iterator

The CDC iterator starts working if snapshot iterator method `HasNext` return false.
The CDC iterator uses snowflake 'stream' (more information about streams
https://docs.snowflake.com/en/user-guide/streams-intro.html).<br>
The change tracking system utilized by the `stream` then records information about the 
DML changes after this snapshot was taken. Change records provide the state
of a row before and after the change. Change information mirrors the column structure
of the tracked source object and includes additional metadata columns that describe each change event.
`Stream` itself does not contain any table data. When we add new row to table after stream creation or stream consuming
this row will be added to stream table. If this row will be removed, this row will be removed from stream table
When row was added to table before stream creation or stream consuming it is not exist in stream table.
If this row will be removed, we will get record about it in stream table.<br>

`Stream` has columns:<br>
`METADATA$ACTION`: Indicates the DML operation (INSERT, DELETE) recorded.<br>
`METADATA$ISUPDATE`: Indicates whether the operation was part of an UPDATE statement.
Updates to rows in the source object are represented as a pair of DELETE and
INSERT records in the stream with a metadata column METADATA$ISUPDATE values set to TRUE.<br>
`METADATA$ROW_ID`: Specifies the unique and immutable ID for the row, which can be used to track changes 
to specific rows over time. <br>

When source starts work first time iterator <b>creates</b> stream with name `conduit_stream_{table}` to `table` from
config, <b>creates</b> table for consuming stream with name `conduit_tracking_{table}`. 
This consuming table has the same schema as `table`  with additional metadata columns:
`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` (Those columns from stream) and `METADATA$TS`.
`METADATA$TS` it is timestamp column, it is special column created by iterator to ordering rows from tracking table. 
Then iterator consume data from stream using insert query to consuming table. `METADATA$TS` sets like current timestamp.
After consuming stream, tracking table has copy of stream data with inserted time. All rows from stream were automatically
removed and stream did new snapshot for `table`.<br>
Iterator run select query for get data from consuming table using limit and offset and ordering by `METADATA$TS`.
Batch size is configurable, offset value is zero for first time.
Iterator save information from table to `data` slice variable. Iterator `HasNext` method check if next element exist in `data` using variable
`index` and if it is needed change offset and run select query to get new data with new offset.
Method `Next` gets next element converts it to `Record` checks action(can be `insert`, `delete`, `update`)
using metadata columns `METADATA$ACTION`, `METADATA$ISUPDATE` and increase `index`.<br>
<b>NOTE:</b> please pay attention and don't accidentally delete stream and tracking table were created by CDC iterator. 

#### Position

Position has fields: `type` (`c` - CDC or `s`- Snapshot), `element`(index of element of current
offset), `offset`. <br> 
Example: 
```
{
 "Type" : "c",
 "Element": 2,
 "Offset": 5
}
```
