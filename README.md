# cassandra-data-loader
For importing large amount of data into C\*. Prepare sstable files using org.apache.cassandra.io.sstable.CQLSSTableWriter. Then use sstableloader to bulk import data into C\*.


## Example Usage

please see [this](http://blog.sws9f.org/nosql/2016/02/11/import-csv-to-cassandra.html) for detail steps.

```bash
sbt "\
run -k myks -t sampledata -p c1,c2 \
-c c1=varchar,c2=double,c3=date \
-f /Users/larrysu/repos/data/test/sample.csv \
-o /Users/larrysu/repos/data/test/sstable/myks/sampledata"

```

```sql
cqlsh> CREATE KEYSPACE myks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
```

```bash
sstableloader -d 127.0.0.1 /Users/larrysu/repos/data/test/sstable/myks/sampledata
```