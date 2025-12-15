package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.SourceStringReader;

import java.util.Collections;
import java.util.Properties;

public class CalciteFacade {
    private final CalciteConnectionConfigImpl config;
    private final RelDataTypeFactory typeFactory;
    private final Prepare.CatalogReader catalogReader;

    public CalciteFacade(CalciteSchema calciteSchema) {
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        this.config = new CalciteConnectionConfigImpl(configProperties);
        this.typeFactory = new JavaTypeFactoryImpl();
        this.catalogReader = new CalciteCatalogReader(
                calciteSchema,
                Collections.singletonList("default"),
                this.typeFactory,
                config);
    }

    // parse
    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(
                new SourceStringReader(sql),
                SqlParser.config()
                        .withCaseSensitive(config.caseSensitive())
                        .withUnquotedCasing(config.unquotedCasing())
                        .withQuotedCasing(config.quotedCasing())
                        .withConformance(config.conformance())
        );
        return sqlParser.parseQuery();
    }

    // validate
    public SqlNode validate(SqlNode sqlNode) {
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                this.catalogReader,
                this.typeFactory,
                SqlValidator.Config.DEFAULT
                        .withDefaultNullCollation(config.defaultNullCollation())
                        .withConformance(config.conformance())
                        .withIdentifierExpansion(true));
        return validator.validate(sqlNode);
    }

    public SqlNode validate(String sql) throws SqlParseException {
        return validate(parse(sql));
    }
}
