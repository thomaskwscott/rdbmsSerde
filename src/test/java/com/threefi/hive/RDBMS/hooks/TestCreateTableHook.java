package com.threefi.hive.RDBMS.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by tscott on 19/06/2017.
 */
public class TestCreateTableHook {

    @Test
    public void createTableConvertExternalTables() {

        CreateTableHook hook = new CreateTableHook();

        String testHiveQuery = "CREATE EXTERNAL TABLE single_col2 (col1 string) ROW FORMAT SERDE \'com.threefi.hive.RDBMS.RDBMSSerde\' STORED AS INPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSInputFormat\' OUTPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSOutputFormat\' LOCATION \'hdfs://host-10-17-101-173.coe.cloudera.com:8020/tmp/rdbmsSerde\'";

        String out = hook.convertCreateQuery(testHiveQuery);

        Assert.assertFalse(out.contains("create external table"));

    }

    @Test
    public void createTableStripsSerdeLocationInputOutputFormat() {

        CreateTableHook hook = new CreateTableHook();

        String testHiveQuery = "CREATE EXTERNAL TABLE default.single_col2 (col1 string) ROW FORMAT SERDE \'com.threefi.hive.RDBMS.RDBMSSerde\' STORED AS INPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSInputFormat\' OUTPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSOutputFormat\' LOCATION \'hdfs://host-10-17-101-173.coe.cloudera.com:8020/tmp/rdbmsSerde\'";

        String out = hook.convertCreateQuery(testHiveQuery);

        Assert.assertFalse(out.contains("outputformat"));
        Assert.assertFalse(out.contains("com.threefi.hive.rdbms.rdbmsoutputformat"));
        Assert.assertFalse(out.contains("inputformat"));
        Assert.assertFalse(out.contains("com.threefi.hive.rdbms.rdbmsinputformat"));
        Assert.assertFalse(out.contains("row format serde"));
        Assert.assertFalse(out.contains("com.threefi.hive.rdbms.rdbmsserde"));
        Assert.assertFalse(out.contains("location"));
        Assert.assertFalse(out.contains("hdfs://host-10-17-101-173.coe.cloudera.com:8020/tmp/rdbmsSerde"));
        Assert.assertEquals("create table default_single_col2 (col1 varchar(4000))",out);

    }

    @Test
    public void createTableStringConvertedToVarchar() {

        CreateTableHook hook = new CreateTableHook();

        String testHiveQuery = "CREATE EXTERNAL TABLE default.single_col2 (col1 string) ROW FORMAT SERDE \'com.threefi.hive.RDBMS.RDBMSSerde\' STORED AS INPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSInputFormat\' OUTPUTFORMAT \'com.threefi.hive.RDBMS.RDBMSOutputFormat\' LOCATION \'hdfs://host-10-17-101-173.coe.cloudera.com:8020/tmp/rdbmsSerde\'";

        String out = hook.convertCreateQuery(testHiveQuery);

        Assert.assertTrue(out.contains("varchar(4000)"));
        Assert.assertFalse(out.contains(" string"));
        Assert.assertEquals("create table default_single_col2 (col1 varchar(4000))",out);

    }

    @Test
    public void pluckDecimalsReplacesDecimals() {
        CreateTableHook hook = new CreateTableHook();

        String testStatement = "CREATE EXTERNAL TABLE default.blah (col1 decimal(4,2), col2 decimal(5,3))";

        String out = hook.pluckDecimals(testStatement);
        String out2 = hook.convertCreateQuery(testStatement);

        Assert.assertEquals("create external table default.blah (col1 decimal**0, col2 decimal**1)",out);
        Assert.assertEquals("create table default_blah (col1 decimal(4,2),col2 decimal(5,3))",out2);



    }


}
