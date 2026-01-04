package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class CustomSchema extends AbstractSchema {
    private final String schemaName;
    private final Map<String, Table> tableMap;

    private CustomSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
