package edu.cmu.cs.db.calcite_app.app;

import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
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

    public static CustomTable create(String tableName, DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName, null);

            List<String> fieldNames = new ArrayList<>();
            List<SqlTypeName> fieldTypes = new ArrayList<>();
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int jdbcDataType = columns.getInt("DATA_TYPE");
                fieldNames.add(columnName);
                fieldTypes.add(SqlTypeName.getNameForJdbcType(jdbcDataType));
            }
            columns.close();

            PreparedStatement countStmt = conn.prepareStatement("SELECT count(*) as row_count FROM " + tableName);
            ResultSet rsCount = countStmt.executeQuery();
            int rowCount = -1;
            if (rsCount.next()) {
                rowCount = rsCount.getInt("row_count");
            }
            rsCount.close();

            return new CustomTable(
                    tableName,
                    fieldNames,
                    fieldTypes,
                    new CustomTableStatistic(rowCount)
            );
        }
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
        throw new UnsupportedOperationException();
    }
}
