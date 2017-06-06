package com.threefi.hive.RDBMS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by tscott on 05/06/2017.
 */

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class RDBMSSerde extends AbstractSerDe {

    private Logger logger = Logger.getLogger(RDBMSSerde.class);

    private String[] columnNames;
    private ObjectInspector inspector;

    @Override
    public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {


        final TypeInfo rowTypeInfo;
        final List<String> columnNames;
        final List<TypeInfo> columnTypes;
        // Get column names and sort order
        final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        this.columnNames = TableUtils.getColumnNames(columnNameProperty);

        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }
        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("ParquetHiveSerde initialization failed. Number of column " +
                    "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
                    columnTypes);
        }
        // Create row related objects
        rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        this.inspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ArrayWritable.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objInspector) throws SerDeException {

        final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();

        if (outputFieldRefs.size() != columnNames.length) {
            throw new SerDeException("Cannot serialize the object because there are "
                    + outputFieldRefs.size() + " fields but the table has " + columnNames.length + " columns.");
        }

        // Get all data out.
        String[] row = new String[columnNames.length];
        for (int c = 0; c < columnNames.length; c++) {
            final Object field = outputRowOI.getStructFieldData(o, outputFieldRefs.get(c));
            final ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();

            // The data must be of type String
            final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;


            row[c] = fieldStringOI.getPrimitiveJavaObject(field);
        }
        ArrayWritable out = new ArrayWritable(row);
        logger.debug("Serialized to:" + out.toString());
        return out;

    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        logger.debug("Deserialized:" + writable);
        return writable;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }
}
