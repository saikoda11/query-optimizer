package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;

import java.util.Properties;

public class CalciteConfig {
    private static CalciteConnectionConfig calciteConnectionConfig;

    public static CalciteConnectionConfig getCalciteConnectionConfig() {
        if (calciteConnectionConfig == null) {
            Properties properties = new Properties();
            properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
            properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            calciteConnectionConfig = new CalciteConnectionConfigImpl(properties);
        }
        return calciteConnectionConfig;
    }
}
