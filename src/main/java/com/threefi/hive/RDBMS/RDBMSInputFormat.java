package com.threefi.hive.RDBMS;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tscott on 11/05/2017.
 *
 * There are already some big caveats to using this:
 *
 * 1. This will not work with the default hive.input.format. This is a compbined input format and, as we only have one
 * split per node it doesn't make any sense to combine anything. Set to HiveInputFormat by running the below before
 * querying rdbmsSerde tables:
 * set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
 *
 * 2. Hive insists that all tables have a HDFS location and file input at least at some level. To work around this
 * rdbmsSerde uses the location /tmp/rdbmsSerde/tmpfile. All tables created that use rdbmsSerde should be external and
 * have their location set to /tmp/rdbmsSerde
 */
public class RDBMSInputFormat<T> implements InputFormat<Void, T> {


    private Logger logger = Logger.getLogger(RDBMSInputFormat.class);


    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {

        String locations = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_NODES);
        logger.info("Discovered locations: " + locations);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        String connectionString = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_CONNECTION_STRING);
        String userName = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_USER_NAME);
        String password = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_PASSWORD);
        String columnNames = jobConf.get(serdeConstants.LIST_COLUMNS);
        String tableName = jobConf.get(hive_metastoreConstants.META_TABLE_NAME);
        String columnTypes = jobConf.get(serdeConstants.LIST_COLUMN_TYPES);
        String filterText = jobConf.get(TableScanDesc.FILTER_TEXT_CONF_STR);

        for(String location : locations.split(","))
        {
            logger.info("adding split with location: " + location);

            splits.add(new RDBMSInputSplit(tableName, columnNames, columnTypes, filterText,location,connectionString,userName, password));
        }

        InputSplit[] finalSplits =  splits.toArray(new InputSplit[splits.size()]);
        return finalSplits;
    }


    @Override
    public RecordReader<Void, T> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {

        return new RDBMSRecordReader<T>(inputSplit);
    }
}
