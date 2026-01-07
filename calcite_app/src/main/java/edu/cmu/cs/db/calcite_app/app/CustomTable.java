package edu.cmu.cs.db.calcite_app.app;

import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CustomTable extends AbstractTable implements ScannableTable {
    @Getter
    private final String tableName;
    @Getter
    private final List<String> fieldNames;
    @Getter
    private final List<SqlTypeName> fieldTypes;
    private final CustomTableStatistic statistic;

    private RelDataType rowType;

    public static CustomTable create(String tableName) throws SQLException {
        Pair<List<String>, List<SqlTypeName>> fieldNamesAndTypes = DatabaseFacade.getInstance().getFieldNamesAndTypes(tableName);
        int rowCount = DatabaseFacade.getInstance().getRowCount(tableName);
        return new CustomTable(
                tableName,
                fieldNamesAndTypes.getKey(),
                fieldNamesAndTypes.getValue(),
                new CustomTableStatistic(rowCount)
        );
    }

    private CustomTable(
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

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        List<RelDataTypeField> fields = this.getRowType(new JavaTypeFactoryImpl()).getFieldList();
        return new CustomEnumerable(tableName, fields);
    }
}
