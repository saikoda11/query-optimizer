package edu.cmu.cs.db.calcite_app.app;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.fail;

public class CustomSchemaTest {
    private final String duckDbFIle = "../input/qop1.db";

    @Test
    void testCreateSuccessWhenExistentDbFile() {
        CustomSchema customSchema = null;
        try {
            customSchema = CustomSchema.create(duckDbFIle);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        assert customSchema.getTableMap() != null && !customSchema.getTableMap().isEmpty();
    }

    @Test
    void testCreateThrowsWhenNonExistentDbFile() {
        Assertions.assertThrows(SQLException.class, () -> {
            CustomSchema.create("../input/imaginary.db");
        });
    }
}
