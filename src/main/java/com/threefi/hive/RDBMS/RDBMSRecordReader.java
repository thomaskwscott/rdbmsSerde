package com.threefi.hive.RDBMS;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
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
    String[] columnNames = null;

    public RDBMSRecordReader(InputSplit split) {
        logger.info("Creating record reader with split:" + split);
        this.split = split;

    }

    private void createResultSet(InputSplit split) throws SQLException, IOException {

        //retrieve query details
        String queryColumns = TableUtils.stripColumnNames(((RDBMSInputSplit) split).getColumnNames());
        columnNames = TableUtils.getColumnNames(((RDBMSInputSplit) split).getColumnNames());
        String tableName = TableUtils.getTableName(((RDBMSInputSplit) split).getTableName());
        String whereClause = ((RDBMSInputSplit) split).getFilterText().length() == 0 ? "" : " where " + ((RDBMSInputSplit) split).getFilterText();
        columnTypes = TableUtils.getColumnTypes(((RDBMSInputSplit) split).getColumnTypes());
        String username = ((RDBMSInputSplit) split).getUserName();
        String connectionString = ((RDBMSInputSplit) split).getConnectionString();
        String password = ((RDBMSInputSplit) split).getPassword();
        logger.debug("Connecting to:" + connectionString + " as user:" + username);

        conn = DriverManager.getConnection(connectionString.replace("[host]", split.getLocations()[0]), username, password);
        Statement stmt = conn.createStatement();

        String sql = "select " + queryColumns + " from " + tableName + " " + whereClause;
        logger.debug("Executing query:" + sql);

        rs = stmt.executeQuery(sql);
        hasConnection = true;

    }

    @Override
    public boolean next(Void aVoid, T t) throws IOException {
        if (!hasConnection) {
            try {
                createResultSet(split);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        try {
            if (rs.next()) {
                Writable[] row = new Writable[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {

                    if (columnTypes[i].equals("string")) {
                        Text stringField = new Text();
                        stringField.set(rs.getString(i + 1));
                        row[i] = stringField;
                    }
                    if (columnTypes[i].equals("int")) {
                        IntWritable intField = new IntWritable();
                        intField.set(rs.getInt(i + 1));
                        row[i] = intField;
                    }
                    if (columnTypes[i].equals("bigint")) {
                        LongWritable longField = new LongWritable();
                        longField.set(rs.getLong(i + 1));
                        row[i] = longField;
                    }
                    if (columnTypes[i].equals("float")) {
                        FloatWritable floatField = new FloatWritable();
                        floatField.set(rs.getFloat(i + 1));
                        row[i] = floatField;
                    }
                    if (columnTypes[i].equals("double")) {
                        DoubleWritable doubleField = new DoubleWritable();
                        doubleField.set(rs.getDouble(i + 1));
                        row[i] = doubleField;
                    }
                    if (columnTypes[i].startsWith("decimal")) {
                        HiveDecimalWritable decimalField = new HiveDecimalWritable();
                        BigDecimal in = rs.getBigDecimal(i + 1);
                        decimalField.set(HiveDecimal.create(in));
                        row[i] = decimalField;
                    }
                }
                ((ArrayWritable) t).set(row);
                return true;
            }
            conn.close();
        } catch (
                SQLException e)

        {
            logger.error("Could not close connection", e);
            throw new IOException("Could not close connection", e);
        }

        hasNext = false;
        return false;
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public T createValue() {
        ArrayWritable out = new ArrayWritable(new String[]{});
        return (T) out;
    }

    @Override
    public long getPos() throws IOException {
        return hasNext ? 0l : 1l;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
        return hasNext ? 0.0f : 1.0f;
    }
}
