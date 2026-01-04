package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class CustomSchema extends AbstractSchema {
    private final String schemaName;
    private final Map<String, Table> tableMap;

    public static CustomSchema create(String duckDbFIlePath) {
        Map<String, Table> tableMap = new HashMap<>();

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        DataSource datasource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIlePath, "org.duckdb.DuckDBDriver", null, null);
        Schema jdbcSchema = JdbcSchema.create(rootSchema.plus(), "qop1", datasource, null, null);

        for (String tableName : jdbcSchema.getTableNames()) {
            CustomTable customTable = CustomTable.create(tableName, datasource);
            tableMap.put(tableName, customTable);
        }
        return new CustomSchema("qop1", tableMap);
    }

    private CustomSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
