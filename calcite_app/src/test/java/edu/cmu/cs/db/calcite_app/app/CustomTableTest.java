package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class CustomTableTest {
    private static final String duckDbFIle = "../input/qop1.db";
    private static DataSource dataSource;

    @BeforeAll
    static void init() {
        dataSource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIle,
                "org.duckdb.DuckDBDriver",
                null,
                null);
    }

    @Test
    void testCreateSuccess() {
        CustomTable customTable = null;
        try {
            customTable = CustomTable.create("lineitem", dataSource);
        } catch (SQLException e) {
            fail(e);
        }
        Assertions.assertEquals(6001215, customTable.getStatistic().getRowCount());
        assert (Objects.equals(customTable.getTableName(), "lineitem"));
        assert !customTable.getFieldTypes().isEmpty();
        assert !customTable.getFieldNames().isEmpty();
        assert customTable.getFieldNames().size() == customTable.getFieldTypes().size();
    }

    @Test
    void testCreateThrowsWhenRelationDoesNotExist() {
        assertThrows(Exception.class, () -> {
            CustomTable.create("nonexistentrelation", dataSource);
        });
    }
}
