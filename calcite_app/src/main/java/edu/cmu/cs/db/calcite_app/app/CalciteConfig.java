package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Properties;

public class CalciteConfig {
    private static CalciteConnectionConfig calciteConnectionConfig;
    private static SqlParser.Config sqlParserConfig;
    private static SqlValidator.Config sqlValidatorConfig;

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

    public static SqlParser.Config getSqlParserConfig() {
        if (sqlParserConfig == null) {
            sqlParserConfig = SqlParser.config()
                    .withCaseSensitive(calciteConnectionConfig.caseSensitive())
                    .withUnquotedCasing(calciteConnectionConfig.unquotedCasing())
                    .withQuotedCasing(calciteConnectionConfig.quotedCasing())
                    .withConformance(calciteConnectionConfig.conformance());
        }
        return sqlParserConfig;
    }

    public static SqlValidator.Config getSqlValidatorConfig() {
        if (sqlValidatorConfig == null) {
            sqlValidatorConfig = SqlValidator.Config.DEFAULT
                    .withLenientOperatorLookup(calciteConnectionConfig.lenientOperatorLookup())
                    .withConformance(calciteConnectionConfig.conformance())
                    .withDefaultNullCollation(calciteConnectionConfig.defaultNullCollation())
                    .withIdentifierExpansion(true);
        }
        return sqlValidatorConfig;
    }
}
