# Conduit Connector Snowflake

### General

The Snowflake connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It provides the source
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
| `key`        | Column name that records should use for their `Key` fields.                                                                                                                                                                                     | yes      | "id"                                                   |
| `limit`      | Count of records in one butch. By default is 1000.                                                                                                                                                                                              | no       | "100"                                                  |

### How to build it

Run `make build`.

### Testing

Run `make test`.

### Snowflake Source

The Snowflake Source Connector connects to a snowflake with the provided configurations, using
`connection`, `table`,`columns`, `key`, `limit` using snowflake driver. Then will call `Configure` to parse the
configurations. After that, the `Open` method is called to start the connection from the provided position get the
data from snowflake db. The `Read` return next record. The `Ack` method 
check if is record with the position was recorded. The `Teardown` do gracefully shutdown.
Snapshot iterator start works first, if snapshot iterator `hasNext` method return false start works CDC iterator. 

#### Snapshot Iterator

Snapshot iterator get data from `table` using `limit` and offset.

#### CDC Iterator

CDC iterator use stream for get data (more information about streams https://docs.snowflake.com/en/user-guide/streams-intro.html) 
When cdc iterator start works, it <b>creates</b> stream with name `conduit_{table}`. Iterator read full stream table filter data by 
insert, updates, delete elements and save indexes for each group.

#### Limitations.

CDC iterator couldn't catch delete, update information about elements which was created after stream creation.

