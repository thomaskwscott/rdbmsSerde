package com.threefi.hive.RDBMS;

import org.apache.derby.impl.store.access.UTF;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by tscott on 16/05/2017.
 */
public class RDBMSInputSplit extends FileSplit implements InputSplit {

    private static final String FAKE_PATH = "/tmp/rdbmsSerde/tmpfile";
    private Logger logger = Logger.getLogger(RDBMSInputSplit.class);
    private String location = "";
    private String columnNames = "";
    private String columnTypes = "";
    private String filterText = "";
    private String tableName = "";
    private String connectionString = "";
    private String userName = "";
    private String password = "";


    public RDBMSInputSplit()
    {
        super(new Path(FAKE_PATH), 0l, 1l, new JobConf());
    }

    public RDBMSInputSplit(String tableName, String columnNames, String columnTypes, String filterText,String location,String connectionString,String userName, String password)
    {
        super(new Path(FAKE_PATH), 0l, 1l, new JobConf());
        this.location = location;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.filterText = filterText;
        this.tableName = tableName;
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public String toString()
    {
        return " Table: " + tableName + " Column Names: " + columnNames +
                " Column Types: " + columnTypes + "Filter Text: " + filterText +
                " Location: " + location + " Connection String: " + connectionString +
                " Username: " + userName + " Password: " + password;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[] {location};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out,this.tableName);
        UTF8.writeString(out,this.columnNames);
        UTF8.writeString(out,this.columnTypes);
        UTF8.writeString(out,this.filterText==null?"":this.filterText);
        UTF8.writeString(out, this.location);
        UTF8.writeString(out,this.connectionString);
        UTF8.writeString(out, this.userName);
        UTF8.writeString(out,this.password);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tableName = UTF8.readString(in);
        this.columnNames = UTF8.readString(in);
        this.columnTypes = UTF8.readString(in);
        this.filterText = UTF8.readString(in);
        this.location = UTF8.readString(in);
        this.connectionString = UTF8.readString(in);
        this.userName = UTF8.readString(in);
        this.password = UTF8.readString(in);
    }

    @Override
    public Path getPath()
    {
        return new Path(FAKE_PATH);
    }

    public String getColumnNames()
    {
        StringBuilder strippedColumnNames = new StringBuilder();
        String[] colummns = columnNames.split(",");

        for(int i=0; i< colummns.length-3;i++)
        {
            strippedColumnNames.append(colummns[i] + ",");
        }
        return new String(strippedColumnNames.deleteCharAt(strippedColumnNames.length()-1));
    }

    public String getTableName()
    {
        return tableName.replace(".","_");
    }

    public String getFilterText()
    {
        return filterText;
    }

    public String[] getColumnTypes()
    {
        int colummnsLength = columnNames.split(",").length-3;
        String[] splitColumnTypes = columnTypes.split(",");
        String [] strippedColumnTypes = new String[colummnsLength];
        for(int i=0; i<colummnsLength;i++)
        {
            strippedColumnTypes[i] = splitColumnTypes[i];
        }
        return strippedColumnTypes;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getUserName()
    {
        return userName;
    }

    public String getPassword()
    {
        return password;
    }
}
