## DB-STREAMS TOOL

A collection of tools to help move data from DB to Streams and vice-versa.


## Export MapR-DB JSON to MapR-ES  
- Provide any MapR-DB JSON table path as input

- Provide a MapR-ES path as output

- You could optionally specify topic / partition / maximum messages to produce.


## Command to run:

```
java -cp db-streams-tool.jar com.mapr.db.tools.CopyTableToStream
```
> Usage: CopyTableToStream -table </path/to/table> -stream </path/to/stream> [Options]

Options:
-h or -help <for usage>
-topic <topic name> [default topic: default]
-partition <partition>
-maxmsgs <maximum messages to produce>

**Example:**
```
java -cp db-streams-tool.jar com.mapr.db.tools.CopyTableToStream -table /business_table -stream /sample_stream -topic topic1 -maxmsgs 1000
```