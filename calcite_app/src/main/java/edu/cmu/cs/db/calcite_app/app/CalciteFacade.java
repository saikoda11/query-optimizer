package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
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

public class CalciteFacade {
    private final CalciteConnectionConfig config = CalciteConfig.getCalciteConnectionConfig();
    private final SqlValidator validator;
    private final SqlToRelConverter sqlToRelConverter;
    private final Optimizer optimizer = new Optimizer();
    private final RelDataTypeFactory typeFactory = CustomSchema.getTypeFactory();
    private final CustomSchema customSchema;

    public CalciteFacade(String duckDbFilePath) {
        customSchema = CustomSchema.create(duckDbFilePath);
        this.validator = createValidator();

        VolcanoPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(
                planner,
                new RexBuilder(typeFactory)
        );

        sqlToRelConverter = createConverter(cluster);
    }

    private SqlValidator createValidator() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(customSchema.getSchemaName(), customSchema);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(customSchema.getSchemaName()),
                typeFactory,
                config);

        SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
                .withDefaultNullCollation(config.defaultNullCollation())
                .withConformance(config.conformance())
                .withIdentifierExpansion(true);

        return SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                sqlValidatorConfig
        );
    }

    private SqlToRelConverter createConverter(RelOptCluster cluster) {
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
//        RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
        return new SqlToRelConverter(
                null,
                validator,
                customSchema.getCatalogReader(),
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser.Config sqlParserConfig = CalciteConfig.getSqlParserConfig();
        SqlParser sqlParser = SqlParser.create(sql, sqlParserConfig);
        return sqlParser.parseQuery();
    }

    public SqlNode validate(SqlNode sqlNode) {
        return validator.validate(sqlNode);
    }

    public RelNode sql2rel(SqlNode sqlNode) {
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, true);
        return root.rel;
    }
}
