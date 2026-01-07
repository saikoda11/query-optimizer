package edu.cmu.cs.db.calcite_app.app;

import lombok.Getter;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CustomSchema extends AbstractSchema {
    @Getter
    private final String schemaName;
    private final Map<String, Table> tableMap;
    @Getter
    private final static RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    @Getter
    private final Prepare.CatalogReader catalogReader;

    public static CustomSchema create(String duckDbFIlePath) throws SQLException {
        Map<String, Table> tableMap = new HashMap<>();

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        if (!Path.of(duckDbFIlePath).toFile().exists()) {
            throw new SQLException("Non existent db file");
        }
        DataSource datasource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIlePath, "org.duckdb.DuckDBDriver", null, null);
        Schema jdbcSchema = JdbcSchema.create(rootSchema.plus(), "qop1", datasource, null, null);

        for (String tableName : jdbcSchema.getTableNames()) {
            CustomTable customTable = CustomTable.create(tableName);
            tableMap.put(tableName, customTable);
        }

        return new CustomSchema("qop1", tableMap);
    }

    private CustomSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        this.catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(schemaName),
                typeFactory,
                CalciteConfig.getCalciteConnectionConfig());
        rootSchema.add(schemaName, this);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
