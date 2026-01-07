package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class CustomEnumerable extends AbstractEnumerable<Object[]> {
    private final String tableName;
    private final List<RelDataTypeField> fields;

    public CustomEnumerable(String tableName, List<RelDataTypeField> fields) {
        this.tableName = tableName;
        this.fields = fields;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return Linq4j.enumerator(DatabaseFacade.getInstance().getTableRecords(tableName, fields));
    }
}
