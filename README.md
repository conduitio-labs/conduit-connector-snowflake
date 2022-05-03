# Conduit Connector Snowflake

### General

The Snowflake connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It provides the source
snowflake connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2

### Configuration

The config passed to `Configure` can contain the following fields.

| name         | description                                                                                                                                            | required | example                                         |
|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------|
| `connection` | Snowflake connection string.<br/>Please use formats:<br/> user:password@my_organization-my_account/mydb <br/> username[:password]@hostname:port/dbname | yes      | "user:password@my_organization-my_account/mydb" |
| `table`      | The table name in snowflake db.                                                                                                                        | yes      | "users"                                         |
| `columns`    | Comma separated list of column names that should be included in each Record's payload.                                                                 | no       | "id,name,age"                                   |
| `key`        | Column name that records should use for their `Key` fields.                                                                                            | yes      | "id"                                            |
| `limit`      | Count of records in one butch. By default is 1000.                                                                                                     | no       | "100"                                           |
