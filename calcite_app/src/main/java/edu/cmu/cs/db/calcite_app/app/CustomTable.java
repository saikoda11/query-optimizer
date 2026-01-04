package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class CustomTable extends AbstractTable {
    private final String tableName;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private final CustomTableStatistic statistic;

    private RelDataType rowType;

    public CustomTable(
            String tableName,
            List<String> fieldNames,
            List<SqlTypeName> fieldTypes,
            CustomTableStatistic statistic
    ) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.statistic = statistic;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }

            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }

        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }
}
