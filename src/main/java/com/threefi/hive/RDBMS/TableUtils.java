package com.threefi.hive.RDBMS;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tscott on 04/06/2017.
 */
public class TableUtils {

    public static String stripColumnNames(String columnNames)
    {
        StringBuilder strippedColumnNames = new StringBuilder();
        String[] columns = columnNames.split(",");

        for(int i=0; i< columns.length;i++)
        {
            if(!columns[i].equals("BLOCK__OFFSET__INSIDE__FILE")
             && !columns[i].equals("INPUT__FILE__NAME")
             && !columns[i].equals("ROW__ID")) {
                strippedColumnNames.append(columns[i] + ",");
            }
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
        List<String> decimals = new ArrayList<String>();
        String in = columnTypes.toLowerCase();
        int decimalCounter=0;
        while(in.contains("decimal(")){
            int startPos = in.indexOf("decimal(");
            String before = in.substring(0,startPos);
            String after = in.substring(startPos);
            String decimalVal = after.substring(0,after.indexOf(")") + 1);
            decimals.add(decimalVal);
            after = after.substring(after.indexOf(")") + 1);
            in = before + "decimal**" + decimalCounter + after;
            decimalCounter++;
        }
        int colummnsLength = in.split(",").length-3;
        String[] splitColumnTypes = in.split(",");
        String [] strippedColumnTypes = new String[colummnsLength];
        for(int i=0; i<colummnsLength;i++)
        {
            if(splitColumnTypes[i].contains("decimal**")) {
                for(int p = 0; p<decimals.size();p++)
                {
                    splitColumnTypes[i] = splitColumnTypes[i].replace("decimal**" + p, decimals.get(p));
                }
            }
            strippedColumnTypes[i] = splitColumnTypes[i];
        }
        return strippedColumnTypes;
    }


    public static String getTableName(String tableName)
    {
        return tableName.replace(".","_");
    }

}
