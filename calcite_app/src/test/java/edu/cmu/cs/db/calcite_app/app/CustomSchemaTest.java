package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.fail;

public class CustomSchemaTest {
    private static final String duckDbFIle = "../input/qop1.db";

    @BeforeAll
    static void init() {
        DataSource dataSource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIle,
                "org.duckdb.DuckDBDriver",
                null,
                null);
        DatabaseFacade.init(dataSource);
    }

    @Test
    void testCreateSuccessWhenExistentDbFile() {
        CustomSchema customSchema = null;
        try {
            customSchema = CustomSchema.create();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        assert customSchema.getTableMap() != null && !customSchema.getTableMap().isEmpty();
    }
}
