package com.threefi.hive.RDBMS.hooks;

import com.threefi.hive.RDBMS.RDBMSSerdeConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by tscott on 19/06/2017.
 */
public class CreateTableHook implements ExecuteWithHookContext {

    private Logger logger = Logger.getLogger(CreateTableHook.class);

    @Override
    public void run(HookContext hookContext) throws Exception {
        assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);

        HiveConf conf = hookContext.getConf();
        String locations = conf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_NODES);

        String queryString = hookContext.getQueryPlan().getQueryStr();
        if(queryString.toLowerCase().contains("stored as inputformat \'com.threefi.hive.rdbms.rdbmsinputformat\'")) {
            // create the table on the underlying rdbms
            for(String location: locations.split(","))
            {
                logger.info("Creating underlying table on " + location + " for query: " + queryString);
                Connection conn = createConnection(conf,location);
                Statement createStatement = conn.createStatement();
                createStatement.executeUpdate(convertCreateQuery(queryString));
                createStatement.close();
                conn.close();
            }

        }


    }

    protected String convertCreateQuery(String origQuery)
    {
        // split out columns section
        StringBuilder builder = new StringBuilder();
        String beforeColumns = origQuery.substring(0,origQuery.indexOf("(")).toLowerCase();
        beforeColumns = beforeColumns.replaceAll("external ","");
        beforeColumns = beforeColumns.replaceAll("\\.","_");
        builder.append(beforeColumns);
        String afterColumns = origQuery.substring(origQuery.indexOf("("));
        String columnSections = afterColumns.substring(0,afterColumns.indexOf(")"));
        StringBuilder colSection = new StringBuilder();
        for(String columnDef : columnSections.split(","))
        {
            String[] defs = columnDef.trim().split("\\s+");
            String columnName = defs[0];
            String columnType = defs[1];
            if(columnType.toLowerCase().equals("string") || columnType.toLowerCase().equals("STRING")) {
                columnType = "varchar(4000)";
            }
            colSection.append(columnName);
            colSection.append(" ");
            colSection.append(columnType);
            colSection.append(",");
        }
        builder.append(colSection.substring(0,colSection.length()-1));
        builder.append(")");

        // strip unneeded sections
        return builder.toString();
    }

    private Connection createConnection(HiveConf jobConf, String rdbmsLocation) throws SQLException, IOException {


        String connectionString = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_CONNECTION_STRING);
        String userName = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_USER_NAME);
        String password = jobConf.get(RDBMSSerdeConstants.HIVE_RDBMS_SERDE_PASSWORD);

        logger.debug("Using RDBMS node:" + rdbmsLocation);

        return DriverManager.getConnection(connectionString.replace("[host]", rdbmsLocation), userName, password);

    }
}
