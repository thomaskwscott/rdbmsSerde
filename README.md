RDBMS Serde 
======

RDBMS Serde swaps the traditional file based storage used by Apache Hive (https://hive.apache.org/) for database tables.  

## Installation/Setup

### Preparation
1. Build this project with mvn clean package
2. Create the directory /tmp/rdbmsSerde (this is not currently configurable) on your HiveServer2 host and on HDFS and copyt the built Jar to it
3. Create an empty file named tmpfile and copy this to /tmp/rdbmsSerde in hdfs (touch tmpfile; hdfs dfs -put tmpfile /tmp/rdbmsSerde;)
4. Configure the following properties in Hive:
<table>
<thead>
<td>Name</td><td>Value</td>
</thead>
<tr>
<td>hive.aux.jars.path<td><td>/tmp/rdbmsSerde</td>
</tr>
<tr>
<td>hive.rdbmsSerde.nodes<td><td>comma seperated list of rdbms nodes (e.g. 10.17.101.175,10.17.101.176,10.17.101.177)</td>
</tr>
<tr>
<td>hive.rdbmsSerde.connectionString<td><td>The connection string used to connect to the RDBMS instances e.g. jdbc:mysql://[host]:3306/rdbmsSerde. [host] is replaced with one of the hosts above as appropriate</td>
</tr>
<tr>
<td>hive.rdbmsSerde.userName<td><td>The username used to connect to the RDBMS instances</td>
</tr>
<tr>
<td>hive.rdbmsSerde.password<td><td>The password used to connect to the RDBMS instances</td>
</tr>
</table>

### Creating Hive tables

All tables using RDBMS serde should be external and us the following template:

CREATE EXTERNAL TABLE `<table name>`(  
    <column spec e.g. `col1` string>  )
ROW FORMAT SERDE
   'com.threefi.hive.RDBMS.RDBMSSerde’
STORED AS INPUTFORMAT
   'com.threefi.hive.RDBMS.RDBMSInputFormat'
OUTPUTFORMAT
   'com.threefi.hive.RDBMS.RDBMSOutputFormat'
LOCATION
   '/tmp/rdbmsSerde'

The table location should always be /tmp/rdbmsSerde as it is not relevant when reading from RDBMS instances

rdbmsSerde requires tables to be created in the underlying databases that provide storage. A post exec hook is provided to automate this. To configure this add the following property:

<table>
<thead>
<td>Name</td><td>Value</td>
</thead>
<tr>
<td>hive.exec.post.hooks<td><td>com.threefi.hive.RDBMS.hooks.CreateTableHook</td>
</tr>
</table>

Important Note: For the post exec hook to work the create table statement must include the database name.

### Supported Types

The following Hive types are supports in RDBMS serde:

- INT
- BIGINT
- FLOAT
- DOUBLE  
- STRING
- DECIMAL
- TIMESTAMP

### Querying the data

Currently Select * type queries with no predicates do not work (as they do not launch mapreduce jobs). To query data add a predicate like the following:
Select * from [table name] where [column name] <= 1 or [column name] > 1

### Inserting data

INSERT INTO statements should work with RDBMSSerde (this is still under development). INSERT OVERWRITE cases do not work.

## Authors and contributors

* Tom Scott

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
