package com.threefi.hive.RDBMS;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * Created by tscott on 04/06/2017.
 */
public class RDBMSRecordWriter implements FileSinkOperator.RecordWriter {

    private Logger logger = Logger.getLogger(RDBMSRecordWriter.class);

    private String[] locations;
    private String connectionString;
    private String userName;
    private String password;
    private String columnNames;
    private String[] columnTypes;
    private String tableName;
    private boolean hasConnection = false;
    Connection conn = null;

    public RDBMSRecordWriter(String connectionString, String userName, String password, String columnNames, String tableName, String columnTypes, String locations)
    {
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
        this.columnNames = TableUtils.stripColumnNames(columnNames);
        this.tableName = TableUtils.getTableName(tableName);
        this.columnTypes = TableUtils.getColumnTypes(columnTypes);
        this.locations = locations.split(",");
    }


    @Override
    public void write(Writable writable) throws IOException {

        if (!hasConnection) {
            try {
                createConnection();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        Writable[] row = ((ArrayWritable) writable).get();
        StringBuilder insertStatementSql = new StringBuilder();
        String sql = "INSERT INTO " + tableName + " ( " + columnNames.replaceAll("tmp_values_", "") + ") values (";

        insertStatementSql.append(sql);
        int numCols = TableUtils.getColumnNames(columnNames).length;
        for (int i = 0; i < numCols; i++) {
            if (i == numCols-1) {
                insertStatementSql.append(getFormattedValue(row[i].toString(),columnTypes[i]) + ");");
            } else {
                insertStatementSql.append(getFormattedValue(row[i].toString(),columnTypes[i]) + ",");
            }
        }

        try {
            Statement insertStatement = conn.createStatement();
            logger.debug("Executing sql:" + insertStatementSql.toString());
            insertStatement.executeUpdate(insertStatementSql.toString());
        } catch (SQLException e) {
            throw new IOException("Could not insert row: " + insertStatementSql,e);
        }

    }

    private String getFormattedValue(String value, String columnType)
    {
        switch(columnType.toUpperCase())
        {
            case "STRING":
                return "'" + value + "'";
            default:
                return value;
        }
    }


    @Override
    public void close(boolean b) throws IOException {
        try {
            if(conn != null )
            {
                conn.close();
            }
        } catch (SQLException e) {
            throw new IOException("Could not close connection",e );
        }
    }

    private void createConnection() throws SQLException, IOException {

        /*
        The writing node should not be required to have a RDBMS instance on it (not all NodeManagers are RDBMS hosts)
        However, if it does we should take advantage of locality.
         */

        // check if this write is on one of the RDBMS nodes or use a random one
        String localIp = InetAddress.getByName("localhost").getHostAddress();
        String chosenLocation = locations[new Random().nextInt(locations.length)];
        for(String location : locations)
        {
            String locationIp = InetAddress.getByName(location).getHostAddress();
            if(locationIp.equals(localIp))
            {
                chosenLocation = location;
                break;
            }
        }
        logger.debug("Using RDBMS node:" + chosenLocation);

        conn = DriverManager.getConnection(connectionString.replace("[host]", chosenLocation), userName, password);
        hasConnection = true;
    }
}
