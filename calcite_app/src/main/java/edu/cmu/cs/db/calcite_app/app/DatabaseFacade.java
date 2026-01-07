package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class DatabaseFacade {
    private static DatabaseFacade instance;
    private final DataSource dataSource;
    private final Map<String, List<Object[]>> tableMap;

    public static void init(DataSource dataSource) {
        if (instance == null) {
            instance = new DatabaseFacade(dataSource);
        }
    }

    public static DatabaseFacade getInstance() {
        if (instance == null) {
            throw new RuntimeException("DataSource was not provided via init method");
        }
        return instance;
    }

    private DatabaseFacade(DataSource dataSource) {
        tableMap = new HashMap<>();
        this.dataSource = dataSource;
    }

    public List<Object[]> getTableRecords(String tableName, List<RelDataTypeField> fields) {
        if (!tableMap.containsKey(tableName)) {
            try {
                loadTable(tableName, fields);
            } catch (SQLException e) {
                System.out.println("COULD NOT LOAD TABLE RECORDS: " + tableName);
                return Collections.emptyList();
            }
        }
        return tableMap.getOrDefault(tableName, Collections.emptyList());
    }

    private void loadTable(String tableName, List<RelDataTypeField> fields) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT * from " + tableName);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                Object[] row = new Object[fields.size()];
                for (int i = 0; i < row.length; i++) {
                    String fieldName = fields.get(i).getName();
                    row[i] = rs.getObject(fieldName);
                }
                rows.add(row);
            }
            tableMap.put(tableName, rows);
        }
    }

    public Pair<List<String>, List<SqlTypeName>> getFieldNamesAndTypes(String tableName) throws SQLException {
        List<String> fieldNames = new ArrayList<>();
        List<SqlTypeName> fieldTypes = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             ResultSet columns = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int jdbcDataType = columns.getInt("DATA_TYPE");
                fieldNames.add(columnName);
                fieldTypes.add(SqlTypeName.getNameForJdbcType(jdbcDataType));
            }
        }
        return Pair.of(fieldNames, fieldTypes);
    }

    public int getRowCount(String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement countStmt = conn.prepareStatement("SELECT count(*) as row_count FROM " + tableName);
             ResultSet rsCount = countStmt.executeQuery();) {
            int rowCount = -1;
            if (rsCount.next()) {
                rowCount = rsCount.getInt("row_count");
            }
            return rowCount;
        }
    }
}
