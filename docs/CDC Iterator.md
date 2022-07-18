# CDC Iterator implementation, issues, ideas.

## General.

Snowflake doesn't have ability to provide information about data changes without stream creation.
A few options for supporting CDC in Snowflake have been discussed and they are mentioned in this document.

### Stream

Some information about steam.
When created, a stream logically takes an initial snapshot of every row in the source object
(e.g. table, external table, or the underlying tables for a view) by initializing a point in time
(called an offset) as the current transactional version of the object.


The change tracking system utilized by the stream then records information about the DML changes 
after this snapshot was taken. Change records provide the state of a row before and after the change.
Change information mirrors the column structure of the tracked source object and includes additional metadata 
columns that describe each change event. Note that a stream itself does not contain any table data. 
A stream only stores an offset for the source object and returns CDC records
by leveraging the versioning history for the source object. 


When the first stream for a table is created, a pair of hidden columns are added 
to the source table and begin storing change tracking metadata. 
These columns consume a small amount of storage. The CDC records returned when querying a stream 
rely on a combination of the offset stored in the stream and the change tracking metadata stored in the table <br>
There are 3 types of stream: Standard, Append-only, Insert-only. For us is interesting only Standard stream.

### Implementation / Issues

First idea was to create only stream to table and read data from stream. It was declined because we must anyway consume
steam for getting fresh and actual data. Also, for us is important to get data by some stable ordering to
right iteration. Then we decided to create tracking table for consuming stream and saving data.
The next problem was ordering in tracking table. Stream doesn't have fields about time creation and when we insert 
new batch data it will insert inside some rows and could break position. For this reason we decided to add special
column to save current timestamp when we consume data and do ordering by this column 


Other idea was to set in config special incrementing column. The main issue for it that the table could be
without this specific column. It also can be the issue with right ordering. Because after interrupted cdc we have to 
continue from position, it is means we must be sure that we have ordering where new updates will be always after 
previous updates.
The final solution with details in readme file https://github.com/conduitio-labs/conduit-connector-snowflake/blob/main/README.md

