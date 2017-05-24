package com.threefi.hive.RDBMS;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;

/**
 * Created by tscott on 11/05/2017.
 */
public class RDBMSRecordReader<T> implements RecordReader<Void, T> {

    private Logger logger = Logger.getLogger(RDBMSInputSplit.class);
    private InputSplit split = null;
    private boolean hasConnection = false;
    private boolean hasNext = true;
    Connection conn = null;
    ResultSet rs = null;
    String[] columnTypes = null;

    public RDBMSRecordReader(InputSplit split) {
        logger.info("Creating record reader with split:" + split);
        this.split = split;

    }

    private void createResultSet(InputSplit split) throws SQLException, IOException {

        //retrieve query details
        String queryColumns = ((RDBMSInputSplit) split).getColumnNames();
        String tableName = ((RDBMSInputSplit) split).getTableName();
        String whereClause = ((RDBMSInputSplit) split).getFilterText().length() == 0 ? "" : " where " + ((RDBMSInputSplit) split).getFilterText();
        columnTypes = ((RDBMSInputSplit) split).getColumnTypes();
        String username = ((RDBMSInputSplit) split).getUserName();
        String connectionString = ((RDBMSInputSplit) split).getConnectionString();
        String password = ((RDBMSInputSplit) split).getPassword();

        conn = DriverManager.getConnection(connectionString.replace("[host]", split.getLocations()[0]), username, password);
        Statement stmt = conn.createStatement();

        rs = stmt.executeQuery("select " + queryColumns + " from " + tableName + " " + whereClause);
        hasConnection = true;

    }

    @Override
    public boolean next(Void aVoid, T t) throws IOException {
        if(!hasConnection){
            try {
                createResultSet(split);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        try {
            if (rs.next()) {

                Writable[] row = new Writable[columnTypes.length];
                for(int i =0; i<columnTypes.length;i++)
                {
                    switch (columnTypes[i])
                    {
                        case "string":
                            Text stringField = new Text();
                            stringField.set(rs.getString(i+1));
                            row[i] = stringField;
                            break;
                        case "int":
                            IntWritable intField = new IntWritable();
                            intField.set(rs.getInt(i+1));
                            row[i] = intField;
                            break;
                    }
                }
                ((ArrayWritable)t).set(row);
                return true;
            }
            conn.close();
        } catch (SQLException e) {
            logger.error("Could not close connection",e);
            throw  new IOException("Could not close connection",e);
        }
        hasNext=false;
        return false;
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public T createValue() {
        ArrayWritable out = new ArrayWritable(new String[] {  });
        return (T)out;
    }

    @Override
    public long getPos() throws IOException {
        return hasNext?0l:1l;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
        return hasNext?0.0f:1.0f;
    }
}
