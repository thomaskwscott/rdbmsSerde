package com.threefi.hive.RDBMS;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by tscott on 02/06/2017.
 */
public class RDBMSOutputFormat<T> implements HiveOutputFormat<Void,T> {

    private Logger logger = Logger.getLogger(RDBMSOutputFormat.class);

    @Override
    public RecordWriter<Void, T> getRecordWriter(FileSystem fileSystem, JobConf jobConf, java.lang.String s, Progressable progressable) throws IOException {
        logger.error("getRecordWriter called when it should not be used");
        throw new IOException("This method should not be used, use getHiveRecordWriter instead");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class<? extends Writable> aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
        String locations = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_NODES);
        String connectionString = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_CONNECTION_STRING);
        String userName = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_USER_NAME);
        String password = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_PASSWORD);
        String columnNames = jobConf.get(serdeConstants.LIST_COLUMNS);
        String tableName = jobConf.get(hive_metastoreConstants.META_TABLE_NAME);
        String columnTypes = jobConf.get(serdeConstants.LIST_COLUMN_TYPES);

        logger.debug("Creating RDBMSRecordWriter with properties: locations:" + locations +
          ",connection string:" + connectionString +
          ",username:" + userName +
          ",columns:" + columnNames +
          ",table:" + tableName +
          ",column types:" + columnTypes
        );

        // for some reason we need to creat an empty file just to fill up the namenode ;-)
        FileSystem fs = path.getFileSystem(jobConf);
        FSDataOutputStream out = fs.create(path);
        out.close();

        return new RDBMSRecordWriter(connectionString,userName,password,columnNames,tableName,columnTypes,locations);
    }
}
