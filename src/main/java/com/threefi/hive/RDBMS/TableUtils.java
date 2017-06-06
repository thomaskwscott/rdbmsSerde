package com.threefi.hive.RDBMS;

import org.apache.log4j.Logger;

/**
 * Created by tscott on 04/06/2017.
 */
public class TableUtils {
    

    public static String stripColumnNames(String columnNames)
    {
        StringBuilder strippedColumnNames = new StringBuilder();
        String[] colummns = columnNames.split(",");

        for(int i=0; i< colummns.length-3;i++)
        {
            strippedColumnNames.append(colummns[i] + ",");
        }
        if(strippedColumnNames.length() > 0) {
            return new String(strippedColumnNames.deleteCharAt(strippedColumnNames.length() - 1));
        }else{
            return "";
        }
    }

    public static String[] getColumnNames(String columnNames)
    {
        return stripColumnNames(columnNames).split(",");
    }

    public static String[] getColumnTypes(String columnTypes)
    {
        int colummnsLength = columnTypes.split(",").length-3;
        String[] splitColumnTypes = columnTypes.split(",");
        String [] strippedColumnTypes = new String[colummnsLength];
        for(int i=0; i<colummnsLength;i++)
        {
            strippedColumnTypes[i] = splitColumnTypes[i];
        }
        return strippedColumnTypes;
    }

    public static String getTableName(String tableName)
    {
        return tableName.replace(".","_");
    }
}
