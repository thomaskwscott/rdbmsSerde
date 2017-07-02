package com.threefi.hive.RDBMS;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tscott on 02/07/2017.
 */
public class TestTableUtils {

    @Test
    public void TestGetColumnTypesIgnoresDecimalCommas() {
        String in = "string,decimal(4,2),decimal(5,3),int,ignored,ignored,ignored";
        String[] out = TableUtils.getColumnTypes(in);

        Assert.assertEquals(4,out.length);
        Assert.assertEquals("string",out[0]);
        Assert.assertEquals("decimal(4,2)",out[1]);
        Assert.assertEquals("decimal(5,3)",out[2]);
        Assert.assertEquals("int",out[3]);


    }
}
