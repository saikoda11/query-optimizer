package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.SourceStringReader;

import java.util.Collections;
import java.util.Properties;

public class CalciteFacade {
    private final CalciteConnectionConfigImpl config;
    private final SqlValidator validator;
    private final SqlToRelConverter sqlToRelConverter;
    private final Optimizer optimizer = new Optimizer();

    public CalciteFacade(CalciteSchema calciteSchema) {
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        this.config = new CalciteConnectionConfigImpl(configProperties);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                calciteSchema,
                Collections.singletonList("default"),
                typeFactory,
                config);

        this.validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT
                        .withDefaultNullCollation(config.defaultNullCollation())
                        .withConformance(config.conformance())
                        .withIdentifierExpansion(true));

        RelOptCluster cluster = RelOptCluster.create(optimizer.getPlanner(), new RexBuilder(typeFactory));

        RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
        this.sqlToRelConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                this.validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config()
                        .withExpand(true));
    }

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

    public SqlNode validate(SqlNode sqlNode) {
        return this.validator.validate(sqlNode);
    }

    public RelNode sql2rel(SqlNode sqlNode) {
        return this.sqlToRelConverter
                .convertQuery(
                        sqlNode, false, true
                ).rel;
    }
}
